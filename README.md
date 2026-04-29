<h1 align="center">FRAGATA</h1>

<p align="center">
  <img src="logo.png" alt="FRAGATA logo" width="300" />
</p>

<p align="center">
  <strong>Semantic retrieval of HPC support tickets via hybrid RAG over 20 years of Request Tracker history.</strong>
</p>

<p align="center">
  <a href="https://arxiv.org/abs/2604.13721" target="_blank">
    <img src="https://img.shields.io/badge/arXiv-2604.13721-b31b1b.svg?style=for-the-badge" alt="arXiv paper">
  </a>
</p>

This repository contains the implementation described in:

> Santiago Paramés-Estévez, Nicolás Filloy-Montesino, Jorge Fernández-Fabeiro, José Carlos Mouriño-Gallego.
> *FRAGATA: Semantic Retrieval of HPC Support Tickets via Hybrid RAG over 20 Years of Request Tracker History.*
> arXiv:2604.13721, 2026. <https://arxiv.org/abs/2604.13721>

The system combines semantic retrieval (FAISS + multilingual sentence-transformers), lexical retrieval (BM25), hybrid fusion (RRF), and cross-encoder reranking to surface relevant past incidents from a CESGA Request Tracker history, regardless of language variation, typos, or different phrasing.

## Architecture

```
FinisTerrae III supercomputer (FT3)
  └─ rag/                  ← this component
       Embedding pipeline, FAISS index build, weekly RT ingestion
       Runs heavy GPU compute via Slurm

VM rag-prod-02
  ├─ rag/                  ← same component, deployed to /opt/rag/
  │    FastAPI backend — search API, async ingestion jobs, engine hot-swap
  └─ rag-frontend-build/   ← static frontend, served by Nginx
       Pre-built HTML/JS/CSS bundle
```

The VM triggers GPU jobs on FT3 over SSH using the `compute` wrapper. After each run, FAISS index artifacts are synced back to the VM and the live engine is hot-swapped without restarting the server.

## Components

| Directory | Runs on | Description |
|---|---|---|
| [`rag/`](rag/) | FT3 (compute) + VM (API) | Retrieval engine, FastAPI backend, ingestion pipeline |
| [`rag-frontend-build/`](rag-frontend-build/) | VM (Nginx) | Pre-built static frontend |

## Deployment overview

### FT3 — compute side

```bash
# Clone and navigate to the rag component
git clone https://github.com/s-parames/fragata/
cd rag-project/rag

# Create virtualenv and install dependencies
python3 -m venv RAG
./RAG/bin/pip install -r requirements.txt

# Copy and fill in the credentials/connection template
cp state/daily_ingest.env.example state/daily_ingest.env
$EDITOR state/daily_ingest.env
```

See [`rag/README.md`](rag/README.md) for the full setup guide.

### VM — API + frontend

```bash
# Deploy rag/ to the VM (run from FT3)
rsync -aP --delete -e "ssh -i ~/.ssh/<KEY>" rag-project/rag/ <vm_user>@<vm_ip>:/opt/rag/

# Build and publish the static frontend
cd /opt/rag
scripts/build_frontend_bundle.sh --api-base-url http://<VM_IP>:8010 --output-dir /tmp/rag-frontend
sudo rsync -a --delete /tmp/rag-frontend/ /var/www/rag-frontend/
```

See [`rag-frontend-build/README.md`](rag-frontend-build/README.md) and `rag/deploy/cloud_srv_cesga/` for Nginx and systemd configuration.

## Citation

```bibtex
@misc{paramesestevez2026fragata,
  title   = {FRAGATA: Semantic Retrieval of HPC Support Tickets via Hybrid RAG over 20 Years of Request Tracker History},
  author  = {Paramés-Estévez, Santiago and Filloy-Montesino, Nicolás and Fernández-Fabeiro, Jorge and Mouriño-Gallego, José Carlos},
  year    = {2026},
  eprint  = {2604.13721},
  archivePrefix = {arXiv},
  primaryClass  = {cs.IR}
}
```

## Acknowledgements

This research project was made possible through the access granted by the Galicia Supercomputing Center (CESGA) to its supercomputing infrastructure. The supercomputer FinisTerrae III and its permanent data storage system have been funded by the NextGeneration EU 2021 Recovery, Transformation and Resilience Plan, ICT2021-006904, and also from the Pluriregional Operational Programme of Spain 2014-2020 of the European Regional Development Fund (ERDF), ICTS-2019-02-CESGA-3, and from the State Programme for the Promotion of Scientific and Technical Research of Excellence of the State Plan for Scientific and Technical Research and Innovation 2013-2016 State subprogramme for scientific and technical infrastructures and equipment of ERDF, CESG15-DE-3114

Additionally, this work was carried out within the framework of the Technological Upgrade Project for the Computing and Data Node of the Galicia Supercomputing Center (CESGA), funded by the Recovery, Transformation and Resilience Plan through the NextGenerationEU instrument of the European Union, within the Strategic Project for Economic Recovery and Transformation in Microelectronics and Semiconductors (PERTE Chip), in accordance with Royal Decree 714/2024.

