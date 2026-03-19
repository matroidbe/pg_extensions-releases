# pg_image Makefile
#
# Standard install:  PG_CONFIG=/path/to/pg_config make install
# Enable detection:  PG_CONFIG=/path/to/pg_config make setup-onnx
# All-in-one:        PG_CONFIG=/path/to/pg_config make install-full
#
# The extension works without ONNX — metadata, EXIF, GPS, transforms,
# and hashing all work immediately after cargo pgrx install.
# Only img_detect() requires ONNX Runtime + a model file.

# Validate PG_CONFIG
ifdef PG_CONFIG
SHAREDIR := $(shell $(PG_CONFIG) --sharedir)
DATADIR  := $(shell $(PG_CONFIG) --pkglibdir | sed 's|/lib$$||')/data
else
SHAREDIR := $(shell pg_config --sharedir 2>/dev/null)
DATADIR  := $(shell pg_config --pkglibdir 2>/dev/null | sed 's|/lib$$||')/data
endif

# Detect if we need sudo (system PostgreSQL where SHAREDIR is not writable)
NEED_SUDO := $(shell test -w $(SHAREDIR) 2>/dev/null && echo "" || echo "sudo")

# Detect if this is a system PostgreSQL (apt-get, yum) that runs as postgres user
# vs user-installed (pgrx, Homebrew) that runs as current user
# System paths: /usr/share/postgresql, /usr/pgsql-*
IS_SYSTEM_PG := $(shell echo $(SHAREDIR) | grep -qE '^/usr/(share/postgresql|pgsql-)' && echo "yes" || echo "")

# postgres user/group for ownership (only used for system PostgreSQL)
PG_USER  ?= postgres
PG_GROUP ?= postgres

# ONNX Runtime version (must be >= 1.23 for ort 2.0.0-rc.11)
ORT_VERSION ?= 1.23.0

# Architecture
ARCH ?= $(shell uname -m | sed 's/aarch64/aarch64/;s/x86_64/x64/')
ORT_PLATFORM ?= linux-$(ARCH)
ORT_TARBALL := onnxruntime-$(ORT_PLATFORM)-$(ORT_VERSION).tgz
ORT_URL := https://github.com/microsoft/onnxruntime/releases/download/v$(ORT_VERSION)/$(ORT_TARBALL)

# Model directory: $(SHAREDIR)/pg_image (same pattern as pg_ml)
MODEL_DIR ?= $(SHAREDIR)/pg_image
DEFAULT_MODEL ?= yolov8n

# HuggingFace model URLs
HF_CLIP_VISION := https://huggingface.co/Qdrant/clip-ViT-B-32-vision/resolve/main/model.onnx
HF_CLIP_TEXT := https://huggingface.co/Qdrant/clip-ViT-B-32-text/resolve/main/model.onnx
HF_VIT_GPT2_BASE := https://huggingface.co/Xenova/vit-gpt2-image-captioning/resolve/main/onnx

.PHONY: all install install-full setup-onnx setup-onnx-runtime setup-model \
        setup-clip setup-caption configure clean help status

all: help

# ============================================================================
# Extension install (standard cargo pgrx)
# ============================================================================

ifdef PG_CONFIG
install:
	cargo pgrx install --pg-config=$(PG_CONFIG) --release $(if $(NEED_SUDO),--sudo,)
else
install:
	@echo "Usage: PG_CONFIG=/path/to/pg_config make install"
	@echo "  or just: cargo pgrx install --pg-config=..."
	@exit 1
endif

# ============================================================================
# Full install: extension + ONNX runtime + model + configure GUC
# ============================================================================

install-full: install setup-onnx

# ============================================================================
# ONNX setup (runtime + model + GUC configuration)
# ============================================================================

setup-onnx: check-sharedir setup-onnx-runtime setup-model setup-clip setup-caption configure
	@echo ""
	@echo "==> Full ONNX setup complete!"
	@echo "    Model dir:   $(MODEL_DIR)"
	@echo "    Detection:   $(DEFAULT_MODEL).onnx"
	@echo "    Embeddings:  clip-vit-b32-vision.onnx, clip-vit-b32-text.onnx"
	@echo "    Captioning:  vit-gpt2-encoder.onnx, vit-gpt2-decoder.onnx"

check-sharedir:
	@if [ -z "$(SHAREDIR)" ]; then \
		echo "Error: Cannot determine SHAREDIR. Set PG_CONFIG or ensure pg_config is in PATH."; \
		exit 1; \
	fi

# Download and install ONNX Runtime shared library
setup-onnx-runtime:
	@if ldconfig -p 2>/dev/null | grep -q "libonnxruntime.so"; then \
		echo "==> ONNX Runtime already installed, skipping."; \
	else \
		echo "==> Downloading ONNX Runtime $(ORT_VERSION) for $(ORT_PLATFORM)..."; \
		cd /tmp && \
		wget -q --show-progress -O $(ORT_TARBALL) $(ORT_URL) && \
		tar xzf $(ORT_TARBALL) && \
		echo "==> Installing to /usr/local/lib..." && \
		sudo cp onnxruntime-$(ORT_PLATFORM)-$(ORT_VERSION)/lib/libonnxruntime.so.$(ORT_VERSION) /usr/local/lib/ && \
		sudo ln -sf /usr/local/lib/libonnxruntime.so.$(ORT_VERSION) /usr/local/lib/libonnxruntime.so.1 && \
		sudo ln -sf /usr/local/lib/libonnxruntime.so.1 /usr/local/lib/libonnxruntime.so && \
		sudo ldconfig && \
		rm -rf /tmp/$(ORT_TARBALL) /tmp/onnxruntime-$(ORT_PLATFORM)-$(ORT_VERSION) && \
		echo "==> ONNX Runtime $(ORT_VERSION) installed."; \
	fi

# Export and install default YOLO model
setup-model: check-sharedir
	@if [ -f "$(MODEL_DIR)/$(DEFAULT_MODEL).onnx" ]; then \
		echo "==> Model $(DEFAULT_MODEL).onnx already exists at $(MODEL_DIR), skipping."; \
	else \
		echo "==> Creating model directory $(MODEL_DIR)..."; \
		$(NEED_SUDO) mkdir -p $(MODEL_DIR); \
		echo "==> Exporting $(DEFAULT_MODEL) to ONNX (requires: pip install ultralytics)..."; \
		python3 -c "from ultralytics import YOLO; YOLO('$(DEFAULT_MODEL).pt').export(format='onnx')" && \
		$(NEED_SUDO) mv $(DEFAULT_MODEL).onnx $(MODEL_DIR)/ && \
		rm -f $(DEFAULT_MODEL).pt && \
		echo "==> Model installed at $(MODEL_DIR)/$(DEFAULT_MODEL).onnx"; \
		if [ -n "$(IS_SYSTEM_PG)" ]; then \
			echo "==> System PostgreSQL detected, setting ownership to $(PG_USER):$(PG_GROUP)..."; \
			sudo chown -R $(PG_USER):$(PG_GROUP) $(MODEL_DIR); \
		fi; \
	fi

# ============================================================================
# CLIP embedding models (vision + text encoder)
# ============================================================================

setup-clip: check-sharedir
	@$(NEED_SUDO) mkdir -p $(MODEL_DIR)
	@if [ -f "$(MODEL_DIR)/clip-vit-b32-vision.onnx" ]; then \
		echo "==> CLIP vision encoder already exists, skipping."; \
	else \
		echo "==> Downloading CLIP ViT-B/32 vision encoder..."; \
		wget -q --show-progress -O /tmp/clip-vit-b32-vision.onnx $(HF_CLIP_VISION) && \
		$(NEED_SUDO) mv /tmp/clip-vit-b32-vision.onnx $(MODEL_DIR)/ && \
		echo "==> Vision encoder installed."; \
	fi
	@if [ -f "$(MODEL_DIR)/clip-vit-b32-text.onnx" ]; then \
		echo "==> CLIP text encoder already exists, skipping."; \
	else \
		echo "==> Downloading CLIP ViT-B/32 text encoder..."; \
		wget -q --show-progress -O /tmp/clip-vit-b32-text.onnx $(HF_CLIP_TEXT) && \
		$(NEED_SUDO) mv /tmp/clip-vit-b32-text.onnx $(MODEL_DIR)/ && \
		echo "==> Text encoder installed."; \
	fi
	@if [ -f "$(MODEL_DIR)/clip-bpe-vocab.json" ]; then \
		echo "==> CLIP tokenizer vocab already exists, skipping."; \
	else \
		echo "==> Generating CLIP BPE tokenizer vocab..."; \
		python3 -c "\
import json; \
from transformers import CLIPTokenizer; \
tok = CLIPTokenizer.from_pretrained('openai/clip-vit-base-patch32'); \
merges = [' '.join(m) for m in tok.bpe_ranks.keys()]; \
json.dump({'vocab': tok.encoder, 'merges': merges}, open('/tmp/clip-bpe-vocab.json', 'w')); \
print('Exported', len(tok.encoder), 'vocab entries and', len(merges), 'merges')" && \
		$(NEED_SUDO) mv /tmp/clip-bpe-vocab.json $(MODEL_DIR)/ && \
		echo "==> Tokenizer vocab installed."; \
	fi
	@if [ -n "$(IS_SYSTEM_PG)" ]; then \
		sudo chown -R $(PG_USER):$(PG_GROUP) $(MODEL_DIR); \
	fi
	@echo "==> CLIP setup complete! img_embed() and img_embed_text() are ready."

# ============================================================================
# ViT-GPT2 captioning models (encoder + decoder)
# ============================================================================

setup-caption: check-sharedir
	@$(NEED_SUDO) mkdir -p $(MODEL_DIR)
	@if [ -f "$(MODEL_DIR)/vit-gpt2-encoder.onnx" ]; then \
		echo "==> ViT-GPT2 encoder already exists, skipping."; \
	else \
		echo "==> Downloading ViT-GPT2 encoder..."; \
		wget -q --show-progress -O /tmp/vit-gpt2-encoder.onnx $(HF_VIT_GPT2_BASE)/encoder_model.onnx && \
		$(NEED_SUDO) mv /tmp/vit-gpt2-encoder.onnx $(MODEL_DIR)/ && \
		echo "==> Encoder installed."; \
	fi
	@if [ -f "$(MODEL_DIR)/vit-gpt2-decoder.onnx" ]; then \
		echo "==> ViT-GPT2 decoder already exists, skipping."; \
	else \
		echo "==> Downloading ViT-GPT2 decoder..."; \
		wget -q --show-progress -O /tmp/vit-gpt2-decoder.onnx $(HF_VIT_GPT2_BASE)/decoder_model_merged.onnx && \
		$(NEED_SUDO) mv /tmp/vit-gpt2-decoder.onnx $(MODEL_DIR)/ && \
		echo "==> Decoder installed."; \
	fi
	@if [ -f "$(MODEL_DIR)/gpt2-vocab.json" ]; then \
		echo "==> GPT2 vocab already exists, skipping."; \
	else \
		echo "==> Generating GPT2 decoder vocab..."; \
		python3 -c "\
import json; \
from transformers import GPT2Tokenizer; \
tok = GPT2Tokenizer.from_pretrained('gpt2'); \
json.dump({'vocab': tok.encoder}, open('/tmp/gpt2-vocab.json', 'w')); \
print('Exported', len(tok.encoder), 'vocab entries')" && \
		$(NEED_SUDO) mv /tmp/gpt2-vocab.json $(MODEL_DIR)/ && \
		echo "==> GPT2 vocab installed."; \
	fi
	@if [ -n "$(IS_SYSTEM_PG)" ]; then \
		sudo chown -R $(PG_USER):$(PG_GROUP) $(MODEL_DIR); \
	fi
	@echo "==> ViT-GPT2 setup complete! img_caption() is ready."

# Write pg_image.model_dir GUC to postgresql.conf
# Detect postgresql.conf: try DATADIR first, then Debian /etc/postgresql/<ver>/main/
PG_VERSION := $(shell $(if $(PG_CONFIG),$(PG_CONFIG),pg_config) --version 2>/dev/null | sed 's/[^0-9]*\([0-9]*\).*/\1/')
PGCONF_CANDIDATES = $(DATADIR)/postgresql.conf /etc/postgresql/$(PG_VERSION)/main/postgresql.conf

configure: check-sharedir
	@PGCONF=""; \
	for candidate in $(PGCONF_CANDIDATES); do \
		if [ -f "$$candidate" ]; then PGCONF="$$candidate"; break; fi; \
	done; \
	if [ -z "$$PGCONF" ]; then \
		echo "==> postgresql.conf not found at any standard location"; \
		echo "    Set the GUC manually:"; \
		echo "    ALTER SYSTEM SET pg_image.model_dir = '$(MODEL_DIR)';"; \
		echo "    SELECT pg_reload_conf();"; \
	elif grep -q "pg_image.model_dir" "$$PGCONF" 2>/dev/null; then \
		echo "==> pg_image.model_dir already configured in $$PGCONF"; \
	else \
		echo "" | $(NEED_SUDO) tee -a "$$PGCONF" >/dev/null; \
		echo "# pg_image: ONNX model directory" | $(NEED_SUDO) tee -a "$$PGCONF" >/dev/null; \
		echo "pg_image.model_dir = '$(MODEL_DIR)'" | $(NEED_SUDO) tee -a "$$PGCONF" >/dev/null; \
		echo "==> Added pg_image.model_dir = '$(MODEL_DIR)' to $$PGCONF"; \
		echo "    Run: SELECT pg_reload_conf(); to apply."; \
	fi

# ============================================================================
# Status and diagnostics
# ============================================================================

status:
	@echo "pg_image status:"
	@echo ""
	@printf "  Extension:     "; \
	if command -v cargo >/dev/null 2>&1; then echo "cargo pgrx available"; else echo "cargo not found"; fi
	@printf "  ONNX Runtime:  "; \
	if ldconfig -p 2>/dev/null | grep -q "libonnxruntime.so"; then \
		ldconfig -p 2>/dev/null | grep "libonnxruntime.so " | head -1 | awk '{print $$NF}'; \
	else \
		echo "NOT INSTALLED (run: make setup-onnx-runtime)"; \
	fi
	@printf "  SHAREDIR:      "; echo "$(SHAREDIR)"
	@printf "  Model dir:     "; \
	if [ -d "$(MODEL_DIR)" ]; then echo "$(MODEL_DIR)"; else echo "$(MODEL_DIR) (NOT CREATED)"; fi
	@printf "  Default model: "; \
	if [ -f "$(MODEL_DIR)/$(DEFAULT_MODEL).onnx" ]; then \
		ls -lh "$(MODEL_DIR)/$(DEFAULT_MODEL).onnx" | awk '{print $$5, $$NF}'; \
	else \
		echo "NOT FOUND (run: make setup-model)"; \
	fi
	@printf "  System PG:     "; \
	if [ -n "$(IS_SYSTEM_PG)" ]; then echo "yes (owner: $(PG_USER):$(PG_GROUP))"; else echo "no"; fi
	@echo ""
	@echo "  Available models:"
	@if [ -d "$(MODEL_DIR)" ]; then \
		ls -lh $(MODEL_DIR)/*.onnx 2>/dev/null | awk '{print "    " $$5 "\t" $$NF}' || echo "    (none)"; \
	else \
		echo "    (model dir does not exist)"; \
	fi

# Clean build artifacts
clean:
	cargo clean

# Show help
help:
	@echo "pg_image Makefile"
	@echo ""
	@echo "The extension installs via standard cargo pgrx install."
	@echo "This Makefile adds ONNX setup for object detection."
	@echo ""
	@echo "Targets:"
	@echo "  make install          Build and install extension (requires PG_CONFIG)"
	@echo "  make setup-onnx       Install ONNX Runtime + all models + configure GUC"
	@echo "  make setup-onnx-runtime  Install ONNX Runtime library only"
	@echo "  make setup-model      Download/export default YOLO model only"
	@echo "  make setup-clip       Download CLIP vision+text encoders + tokenizer vocab"
	@echo "  make setup-caption    Download ViT-GPT2 encoder+decoder + GPT2 vocab"
	@echo "  make configure        Write pg_image.model_dir to postgresql.conf"
	@echo "  make install-full     Extension + ONNX setup (requires PG_CONFIG)"
	@echo "  make status           Show installation status"
	@echo "  make clean            Clean build artifacts"
	@echo ""
	@echo "Variables:"
	@echo "  PG_CONFIG       Path to pg_config (required for install)"
	@echo "  ORT_VERSION     ONNX Runtime version (default: $(ORT_VERSION))"
	@echo "  MODEL_DIR       Model directory (default: SHAREDIR/pg_image)"
	@echo "  DEFAULT_MODEL   Model name without .onnx (default: $(DEFAULT_MODEL))"
	@echo "  PG_USER         Postgres user for ownership (default: $(PG_USER))"
	@echo "  PG_GROUP        Postgres group for ownership (default: $(PG_GROUP))"
	@echo ""
	@echo "Examples:"
	@echo "  # Standard install (works without ONNX)"
	@echo "  PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make install"
	@echo ""
	@echo "  # Enable object detection"
	@echo "  PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make setup-onnx"
	@echo ""
	@echo "  # All-in-one"
	@echo "  PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make install-full"
	@echo ""
	@echo "  # Use a larger YOLO model"
	@echo "  PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config DEFAULT_MODEL=yolov8s make setup-model"
