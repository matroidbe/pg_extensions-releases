use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use crate::error::PgImageError;

// =============================================================================
// CLIP BPE Tokenizer
// =============================================================================

const CLIP_CONTEXT_LENGTH: usize = 77;
const SOT_TOKEN: i64 = 49406; // <|startoftext|>
const EOT_TOKEN: i64 = 49407; // <|endoftext|>

/// Simple CLIP BPE tokenizer.
///
/// Loads a JSON file with "vocab" (token→id) and "merges" (BPE merge rules).
pub struct ClipTokenizer {
    encoder: HashMap<String, i64>,
    bpe_ranks: HashMap<(String, String), usize>,
}

/// JSON format for the tokenizer vocabulary file.
#[derive(serde::Deserialize)]
struct TokenizerData {
    vocab: HashMap<String, i64>,
    merges: Vec<String>,
}

impl ClipTokenizer {
    /// Load tokenizer from a JSON file containing "vocab" and "merges".
    pub fn load(path: &std::path::Path) -> Result<Self, PgImageError> {
        let data = std::fs::read_to_string(path).map_err(|e| {
            PgImageError::TokenizerError(format!("failed to read tokenizer file: {}", e))
        })?;
        let parsed: TokenizerData = serde_json::from_str(&data).map_err(|e| {
            PgImageError::TokenizerError(format!("failed to parse tokenizer JSON: {}", e))
        })?;

        let mut bpe_ranks = HashMap::new();
        for (i, merge) in parsed.merges.iter().enumerate() {
            let parts: Vec<&str> = merge.split(' ').collect();
            if parts.len() == 2 {
                bpe_ranks.insert((parts[0].to_string(), parts[1].to_string()), i);
            }
        }

        Ok(ClipTokenizer {
            encoder: parsed.vocab,
            bpe_ranks,
        })
    }

    /// Encode text to CLIP token IDs (padded/truncated to 77 tokens).
    ///
    /// Returns a Vec of i64 with exactly CLIP_CONTEXT_LENGTH elements:
    /// [SOT, token1, token2, ..., EOT, 0, 0, ...]
    pub fn encode(&self, text: &str) -> Result<Vec<i64>, PgImageError> {
        let cleaned = text.to_lowercase().trim().to_string();
        let words = self.pre_tokenize(&cleaned);

        let mut tokens = vec![SOT_TOKEN];

        for word in &words {
            let word_with_end = format!("{}</w>", word);
            let bpe_tokens = self.bpe(&word_with_end);
            for token in bpe_tokens {
                if let Some(&id) = self.encoder.get(&token) {
                    tokens.push(id);
                }
                // Skip unknown tokens (CLIP vocab is large enough for most text)
            }
        }

        // Truncate to leave room for EOT
        if tokens.len() >= CLIP_CONTEXT_LENGTH {
            tokens.truncate(CLIP_CONTEXT_LENGTH - 1);
        }
        tokens.push(EOT_TOKEN);

        // Pad to exactly CLIP_CONTEXT_LENGTH
        tokens.resize(CLIP_CONTEXT_LENGTH, 0);
        Ok(tokens)
    }

    /// Simple pre-tokenization: split on whitespace and punctuation.
    fn pre_tokenize(&self, text: &str) -> Vec<String> {
        let mut words = Vec::new();
        let mut current = String::new();

        for ch in text.chars() {
            if ch.is_alphanumeric() || ch == '\'' {
                current.push(ch);
            } else {
                if !current.is_empty() {
                    words.push(current.clone());
                    current.clear();
                }
                if !ch.is_whitespace() {
                    words.push(ch.to_string());
                }
            }
        }
        if !current.is_empty() {
            words.push(current);
        }
        words
    }

    /// Apply BPE to a single word (with </w> suffix).
    fn bpe(&self, word: &str) -> Vec<String> {
        // Split into characters
        let mut pieces: Vec<String> = word.chars().map(|c| c.to_string()).collect();

        if pieces.len() <= 1 {
            return pieces;
        }

        loop {
            // Find the pair with the lowest rank (highest priority merge)
            let mut best_pair = None;
            let mut best_rank = usize::MAX;

            for i in 0..pieces.len() - 1 {
                let pair = (pieces[i].clone(), pieces[i + 1].clone());
                if let Some(&rank) = self.bpe_ranks.get(&pair) {
                    if rank < best_rank {
                        best_rank = rank;
                        best_pair = Some(pair);
                    }
                }
            }

            let Some(pair) = best_pair else {
                break;
            };

            // Merge all occurrences of this pair
            let mut new_pieces = Vec::new();
            let mut i = 0;
            while i < pieces.len() {
                if i < pieces.len() - 1 && pieces[i] == pair.0 && pieces[i + 1] == pair.1 {
                    new_pieces.push(format!("{}{}", pair.0, pair.1));
                    i += 2;
                } else {
                    new_pieces.push(pieces[i].clone());
                    i += 1;
                }
            }
            pieces = new_pieces;

            if pieces.len() == 1 {
                break;
            }
        }

        pieces
    }
}

// =============================================================================
// GPT2 Token Decoder (for captioning output)
// =============================================================================

/// GPT2 vocabulary for decoding token IDs back to text.
pub struct Gpt2Decoder {
    decoder: HashMap<i64, String>,
}

#[derive(serde::Deserialize)]
struct Gpt2VocabData {
    vocab: HashMap<String, i64>,
}

impl Gpt2Decoder {
    pub fn load(path: &std::path::Path) -> Result<Self, PgImageError> {
        let data = std::fs::read_to_string(path).map_err(|e| {
            PgImageError::TokenizerError(format!("failed to read GPT2 vocab: {}", e))
        })?;
        let parsed: Gpt2VocabData = serde_json::from_str(&data).map_err(|e| {
            PgImageError::TokenizerError(format!("failed to parse GPT2 vocab JSON: {}", e))
        })?;

        let decoder: HashMap<i64, String> = parsed.vocab.into_iter().map(|(k, v)| (v, k)).collect();
        Ok(Gpt2Decoder { decoder })
    }

    /// Decode token IDs back to text.
    pub fn decode(&self, tokens: &[i64]) -> String {
        let mut text = String::new();
        for &id in tokens {
            if let Some(token) = self.decoder.get(&id) {
                // GPT2 uses Ġ (U+0120) for leading space
                let cleaned = token.replace('\u{0120}', " ");
                // Remove </w> suffix from BPE
                let cleaned = cleaned.replace("</w>", "");
                text.push_str(&cleaned);
            }
        }
        text.trim().to_string()
    }
}

// =============================================================================
// Global Cached Instances
// =============================================================================

static CLIP_TOKENIZER: OnceLock<Result<ClipTokenizer, String>> = OnceLock::new();
static GPT2_DECODER: OnceLock<Result<Gpt2Decoder, String>> = OnceLock::new();

/// Get or initialize the CLIP tokenizer (loaded once per backend).
pub fn get_clip_tokenizer() -> Result<&'static ClipTokenizer, PgImageError> {
    let result = CLIP_TOKENIZER.get_or_init(|| {
        let dir = crate::detect::MODEL_DIR
            .get()
            .map(|cs| cs.to_string_lossy().into_owned())
            .ok_or_else(|| "pg_image.model_dir is not set".to_string())?;
        let path = PathBuf::from(dir).join("clip-bpe-vocab.json");
        ClipTokenizer::load(&path).map_err(|e| e.to_string())
    });

    match result {
        Ok(tok) => Ok(tok),
        Err(e) => Err(PgImageError::TokenizerError(e.clone())),
    }
}

/// Get or initialize the GPT2 decoder (loaded once per backend).
pub fn get_gpt2_decoder() -> Result<&'static Gpt2Decoder, PgImageError> {
    let result = GPT2_DECODER.get_or_init(|| {
        let dir = crate::detect::MODEL_DIR
            .get()
            .map(|cs| cs.to_string_lossy().into_owned())
            .ok_or_else(|| "pg_image.model_dir is not set".to_string())?;
        let path = PathBuf::from(dir).join("gpt2-vocab.json");
        Gpt2Decoder::load(&path).map_err(|e| e.to_string())
    });

    match result {
        Ok(dec) => Ok(dec),
        Err(e) => Err(PgImageError::TokenizerError(e.clone())),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_tokenizer() -> ClipTokenizer {
        let mut vocab = HashMap::new();
        vocab.insert("a</w>".to_string(), 320);
        vocab.insert("b</w>".to_string(), 321);
        vocab.insert("ab</w>".to_string(), 322);
        vocab.insert("hello</w>".to_string(), 1000);
        vocab.insert("h".to_string(), 100);
        vocab.insert("e".to_string(), 101);
        vocab.insert("l".to_string(), 102);
        vocab.insert("lo</w>".to_string(), 103);
        vocab.insert("he".to_string(), 200);
        vocab.insert("ll".to_string(), 201);
        vocab.insert("hel".to_string(), 300);
        vocab.insert("hell".to_string(), 400);
        vocab.insert("hello".to_string(), 500);

        let mut bpe_ranks = HashMap::new();
        bpe_ranks.insert(("h".to_string(), "e".to_string()), 0);
        bpe_ranks.insert(("l".to_string(), "l".to_string()), 1);
        bpe_ranks.insert(("he".to_string(), "ll".to_string()), 2);
        bpe_ranks.insert(("hell".to_string(), "o".to_string()), 3);
        bpe_ranks.insert(("hello".to_string(), "</w>".to_string()), 4);

        ClipTokenizer {
            encoder: vocab,
            bpe_ranks,
        }
    }

    #[test]
    fn test_encode_pads_to_77() {
        let tok = make_test_tokenizer();
        let result = tok.encode("hello").unwrap();
        assert_eq!(result.len(), CLIP_CONTEXT_LENGTH);
    }

    #[test]
    fn test_encode_starts_with_sot() {
        let tok = make_test_tokenizer();
        let result = tok.encode("hello").unwrap();
        assert_eq!(result[0], SOT_TOKEN);
    }

    #[test]
    fn test_encode_has_eot() {
        let tok = make_test_tokenizer();
        let result = tok.encode("hello").unwrap();
        // EOT should appear after the tokens, before padding
        assert!(result.contains(&EOT_TOKEN));
    }

    #[test]
    fn test_encode_padding_is_zeros() {
        let tok = make_test_tokenizer();
        let result = tok.encode("hello").unwrap();
        // Last elements should be padding zeros
        assert_eq!(result[CLIP_CONTEXT_LENGTH - 1], 0);
    }

    #[test]
    fn test_pre_tokenize_simple() {
        let tok = make_test_tokenizer();
        let words = tok.pre_tokenize("hello world");
        assert_eq!(words, vec!["hello", "world"]);
    }

    #[test]
    fn test_pre_tokenize_punctuation() {
        let tok = make_test_tokenizer();
        let words = tok.pre_tokenize("hello, world!");
        assert_eq!(words, vec!["hello", ",", "world", "!"]);
    }

    #[test]
    fn test_gpt2_decoder() {
        let mut vocab = HashMap::new();
        vocab.insert("hello".to_string(), 1);
        vocab.insert("\u{0120}world".to_string(), 2);

        let decoder = Gpt2Decoder {
            decoder: vocab.into_iter().map(|(k, v)| (v, k)).collect(),
        };
        let text = decoder.decode(&[1, 2]);
        assert_eq!(text, "hello world");
    }
}
