#!/usr/bin/env bash
#
# Download LDBC Graphalytics datasets.
#
# By default downloads tiny (XS) example datasets for correctness testing.
# Use --size S to download S-size datasets for performance benchmarking.
#
# Usage:
#   ./scripts/download_graphalytics.sh                     # XS datasets
#   ./scripts/download_graphalytics.sh --size S            # S-size datasets
#   ./scripts/download_graphalytics.sh --size all          # Both XS and S
#   ./scripts/download_graphalytics.sh --data-dir /path    # Custom output dir

set -euo pipefail

# ── Parse arguments ──────────────────────────────────────────────────
DATA_DIR="data/graphalytics"
SIZE="XS"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --size)
            SIZE="${2:-XS}"
            shift 2
            ;;
        --data-dir)
            DATA_DIR="${2}"
            shift 2
            ;;
        *)
            # Legacy positional arg for data dir
            DATA_DIR="$1"
            shift
            ;;
    esac
done

echo "================================================================"
echo "  LDBC Graphalytics Dataset Downloader"
echo "================================================================"
echo ""
echo "  Target directory: ${DATA_DIR}"
echo "  Size:             ${SIZE}"
echo ""

mkdir -p "${DATA_DIR}"

# ── XS (example) dataset helper ─────────────────────────────────────
download_xs_dataset() {
    local name="$1"
    local dir="${DATA_DIR}/${name}"

    if [ -f "${dir}/${name}.v" ] && [ -f "${dir}/${name}.e" ]; then
        echo "  [SKIP] ${name} — already exists"
        return
    fi

    echo "  [DOWNLOAD] ${name}..."
    mkdir -p "${dir}"

    local base_url="https://raw.githubusercontent.com/ldbc/ldbc_graphalytics/main/graphalytics-validation/src/main/resources/validation-graphs/example"

    # Download vertex file
    if curl -fsSL "${base_url}/${name}.v" -o "${dir}/${name}.v" 2>/dev/null; then
        local vcount
        vcount=$(wc -l < "${dir}/${name}.v" | tr -d ' ')
        echo "    Vertices: ${vcount}"
    else
        echo "    WARNING: Could not download ${name}.v"
    fi

    # Download edge file
    if curl -fsSL "${base_url}/${name}.e" -o "${dir}/${name}.e" 2>/dev/null; then
        local ecount
        ecount=$(wc -l < "${dir}/${name}.e" | tr -d ' ')
        echo "    Edges:    ${ecount}"
    else
        echo "    WARNING: Could not download ${name}.e"
    fi

    # Download properties file from config-template
    local props_url="https://raw.githubusercontent.com/ldbc/ldbc_graphalytics/main/config-template/graphs/${name}.properties"
    if curl -fsSL "${props_url}" -o "${dir}/${name}.properties" 2>/dev/null; then
        echo "    Properties file downloaded"
    fi

    # Download algorithm-specific input/output files for validation
    for algo in BFS CDLP LCC PR SSSP WCC; do
        local algo_url="${base_url}/${name}-${algo}"
        if curl -fsSL "${algo_url}" -o "${dir}/${name}-${algo}" 2>/dev/null; then
            : # silently download
        fi
    done

    # Download algorithm-specific input parameters
    local input_url="${base_url}/${name}-input"
    if curl -fsSL "${input_url}" -o "${dir}/${name}-input" 2>/dev/null; then
        echo "    Input parameters downloaded"
    fi

    echo "    Done: ${dir}/"
}

# ── S-size dataset helper ────────────────────────────────────────────
#
# S-size datasets are distributed as tar.zst archives from the LDBC
# Graphalytics data repository.  Requires `zstd` for decompression.
#
# Datasets:
#   wiki-Talk       ~  2.4M edges,  2.4M vertices (directed)
#   cit-Patents     ~  16.5M edges, 3.8M vertices (directed)
#   datagen-7_5-fb  ~  34.2M edges, 633K vertices (undirected)
#
S_DATASETS=("wiki-Talk" "cit-Patents" "datagen-7_5-fb")
S_BASE_URL="https://ldbcouncil.org/ldbc_graphalytics/datasets"

download_s_dataset() {
    local name="$1"
    local dir="${DATA_DIR}/${name}"

    if [ -f "${dir}/${name}.v" ] && [ -f "${dir}/${name}.e" ]; then
        echo "  [SKIP] ${name} — already exists"
        return
    fi

    # Check for zstd
    if ! command -v zstd &>/dev/null; then
        echo "  [ERROR] zstd not found. Install with: brew install zstd (macOS) or apt install zstd (Linux)"
        return 1
    fi

    echo "  [DOWNLOAD] ${name} (S-size)..."
    mkdir -p "${dir}"

    local archive="${DATA_DIR}/${name}.tar.zst"
    local url="${S_BASE_URL}/${name}.tar.zst"

    # Download archive
    if curl -fSL --progress-bar "${url}" -o "${archive}"; then
        echo "    Downloaded: $(du -h "${archive}" | cut -f1)"
    else
        echo "    WARNING: Could not download ${name}.tar.zst from ${url}"
        echo "    Trying alternate URL..."
        local alt_url="https://datasets.ldbcouncil.org/graphalytics/${name}.tar.zst"
        if curl -fSL --progress-bar "${alt_url}" -o "${archive}"; then
            echo "    Downloaded: $(du -h "${archive}" | cut -f1)"
        else
            echo "    ERROR: Could not download ${name}.tar.zst"
            rm -f "${archive}"
            return 1
        fi
    fi

    # Decompress and extract
    echo "    Decompressing..."
    zstd -d "${archive}" -o "${DATA_DIR}/${name}.tar" --force 2>/dev/null
    tar -xf "${DATA_DIR}/${name}.tar" -C "${DATA_DIR}/"

    # Clean up archive files
    rm -f "${archive}" "${DATA_DIR}/${name}.tar"

    # Verify extraction
    if [ -f "${dir}/${name}.v" ] && [ -f "${dir}/${name}.e" ]; then
        local vcount ecount
        vcount=$(wc -l < "${dir}/${name}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${name}.e" | tr -d ' ')
        echo "    Vertices: ${vcount}"
        echo "    Edges:    ${ecount}"
    else
        echo "    WARNING: Expected files not found after extraction"
        echo "    Contents of ${dir}:"
        ls -la "${dir}/" 2>/dev/null || echo "      (directory not found)"
    fi

    echo "    Done: ${dir}/"
}

# ── Download datasets ────────────────────────────────────────────────

if [[ "${SIZE}" == "XS" || "${SIZE}" == "all" ]]; then
    echo "Downloading XS (example) datasets..."
    echo ""
    download_xs_dataset "example-directed"
    download_xs_dataset "example-undirected"
    echo ""
fi

if [[ "${SIZE}" == "S" || "${SIZE}" == "all" ]]; then
    echo "Downloading S-size datasets..."
    echo ""
    for ds in "${S_DATASETS[@]}"; do
        download_s_dataset "${ds}" || true
    done
    echo ""
fi

# ── Verify ───────────────────────────────────────────────────────────
echo "Verifying datasets..."
echo ""

# Check XS datasets
for ds in example-directed example-undirected; do
    dir="${DATA_DIR}/${ds}"
    if [ -f "${dir}/${ds}.v" ] && [ -f "${dir}/${ds}.e" ]; then
        vcount=$(wc -l < "${dir}/${ds}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${ds}.e" | tr -d ' ')
        echo "  ${ds} (XS):"
        echo "    Vertex file: ${dir}/${ds}.v  (${vcount} lines)"
        echo "    Edge file:   ${dir}/${ds}.e  (${ecount} lines)"
    fi
done

# Check S datasets
for ds in "${S_DATASETS[@]}"; do
    dir="${DATA_DIR}/${ds}"
    if [ -f "${dir}/${ds}.v" ] && [ -f "${dir}/${ds}.e" ]; then
        vcount=$(wc -l < "${dir}/${ds}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${ds}.e" | tr -d ' ')
        echo "  ${ds} (S):"
        echo "    Vertex file: ${dir}/${ds}.v  (${vcount} lines)"
        echo "    Edge file:   ${dir}/${ds}.e  (${ecount} lines)"
    fi
done

echo ""
echo "================================================================"
echo "  Download complete!"
echo ""
echo "  Run benchmarks:"
echo "    cargo bench --release --bench graphalytics_benchmark -- --all"
if [[ "${SIZE}" == "S" || "${SIZE}" == "all" ]]; then
    echo "    cargo bench --release --bench graphalytics_benchmark -- --size S --all"
fi
echo "================================================================"
