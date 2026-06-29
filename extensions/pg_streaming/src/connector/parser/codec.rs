//! Compression codec decoding for parser input bytes.
//!
//! Decompresses a complete payload in memory. For streaming reads of huge
//! files, future work will add a streaming codec applied to `AsyncRead`
//! before the parser sees the bytes.

use crate::connector::sdk::Codec;
use bytes::Bytes;
use flate2::read::GzDecoder;
use std::io::Read;

/// Decode `bytes` according to `codec`. Pass-through for `Codec::None`.
pub fn decode(bytes: Bytes, codec: Codec) -> Result<Bytes, String> {
    match codec {
        Codec::None => Ok(bytes),
        Codec::Gzip => {
            let mut decoder = GzDecoder::new(bytes.as_ref());
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("gzip decode failed: {}", e))?;
            Ok(Bytes::from(out))
        }
        Codec::Zstd => Err("zstd codec not yet implemented in this build".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn gzip(input: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn codec_none_is_passthrough() {
        let input = Bytes::from_static(b"hello world");
        let out = decode(input.clone(), Codec::None).unwrap();
        assert_eq!(out, input);
    }

    #[test]
    fn codec_gzip_roundtrip() {
        let plain = b"plain text payload";
        let compressed = gzip(plain);
        let out = decode(Bytes::from(compressed), Codec::Gzip).unwrap();
        assert_eq!(out.as_ref(), plain);
    }

    #[test]
    fn codec_gzip_handles_empty() {
        let compressed = gzip(b"");
        let out = decode(Bytes::from(compressed), Codec::Gzip).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn codec_gzip_rejects_invalid_bytes() {
        let bad = Bytes::from_static(b"not gzip");
        let err = decode(bad, Codec::Gzip).unwrap_err();
        assert!(err.contains("gzip decode failed"));
    }

    #[test]
    fn codec_zstd_unsupported_yet() {
        let err = decode(Bytes::from_static(b""), Codec::Zstd).unwrap_err();
        assert!(err.contains("zstd"));
    }
}
