# Bitácora operativa VM RAG (frontend) - 2026-03-27

## 1) Objetivo de esta sesión

Desplegar el frontend de RAG en una VM de `cloud.srv.cesga.es` y dejar trazado cómo conectarse desde:

- Windows (usuario),
- FT3 (`login210-19`),
- y FT3 -> VM (ruta usada por Codex para operar en remoto).

## 2) Datos concretos usados

- Fecha: `2026-03-27`
- Proyecto en FT3: `/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag`
- Host FT3 público: `ft3.cesga.es`
- Usuario FT3: `tec_app2`
- VM hostname: `rag-prod-02`
- VM IP privada: `10.38.29.165`
- Usuario VM: `cesgaxuser`
- Clave privada en Windows: `C:\Users\tec_app2\.ssh\rag_cesga`
- Clave copiada a FT3: `~/.ssh/rag_cesga` (permisos `600`)
- Floating IP pública: pendiente de anotar aquí cuando quede fija.

## 3) Conectividad y seguridad (estado trabajado)

Security Group comentado durante la sesión:

- `ingress tcp 80 80 0.0.0.0/0`
- `ingress tcp 443 443 0.0.0.0/0`
- `ingress tcp 22 22 193.144.35.5/32` (más seguro) o `0.0.0.0/0` (abierto, no recomendado para producción)
- `egress IPv4 0.0.0.0/0`
- `egress IPv6 ::/0`

## 4) Qué pasó y cómo se resolvió

1. Desde FT3 se intentó entrar a la VM:

```bash
ssh cesgaxuser@10.38.29.165
```

Error inicial: `Permission denied (publickey)` porque en FT3 no estaba la clave privada.

2. Desde Windows no resolvía `login210-19`:

```text
Could not resolve hostname login210-19
```

Solución: usar el host público `ft3.cesga.es`.

3. Se copió la clave a FT3 y se ajustaron permisos:

```bash
ssh tec_app2@ft3.cesga.es "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
scp "C:\Users\tec_app2\.ssh\rag_cesga" tec_app2@ft3.cesga.es:~/.ssh/rag_cesga
ssh tec_app2@ft3.cesga.es
chmod 600 ~/.ssh/rag_cesga
```

4. Conexión correcta FT3 -> VM:

```bash
ssh -i ~/.ssh/rag_cesga cesgaxuser@10.38.29.165
```

## 5) Comandos de conexión reutilizables

### 5.1 Windows -> FT3

```powershell
ssh tec_app2@ft3.cesga.es
```

### 5.2 FT3 -> VM

```bash
ssh -i ~/.ssh/rag_cesga cesgaxuser@10.38.29.165
```

### 5.3 Windows -> VM pasando por FT3 (jump)

```powershell
ssh -J tec_app2@ft3.cesga.es -i "C:\Users\tec_app2\.ssh\rag_cesga" cesgaxuser@10.38.29.165
```

## 6) Sincronización del código FT3 -> VM

Comando utilizado:

```bash
rsync -aP --info=progress2 --delete -e "ssh -i ~/.ssh/rag_cesga" \
  /mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag/ \
  cesgaxuser@10.38.29.165:/opt/rag/
```

Nota de interpretación importante:

- Ver `48%` con `to-chk=0/56614` significa que terminó de revisar/transferir todo lo necesario.
- `to-chk=0` es la señal de finalización en ese output.

## 7) Despliegue del frontend en la VM

Dentro de la VM (`cesgaxuser@rag-prod-02`):

1. Instalar base:

```bash
sudo apt update
sudo apt install -y nginx rsync git
sudo mkdir -p /opt/rag /var/www/rag-frontend
sudo chown -R "$USER":"$USER" /opt/rag /var/www/rag-frontend
```

2. Generar frontend estático:

```bash
cd /opt/rag
scripts/build_frontend_bundle.sh --output-dir /tmp/rag-frontend
```

Si backend está separado:

```bash
scripts/build_frontend_bundle.sh --api-base-url http://<BACKEND_HOST>:8010 --output-dir /tmp/rag-frontend
```

3. Publicar en Nginx:

```bash
sudo rsync -a --delete /tmp/rag-frontend/ /var/www/rag-frontend/
sudo cp /opt/rag/deploy/cloud_srv_cesga/nginx/rag-frontend.conf /etc/nginx/sites-available/rag-frontend.conf
sudo sed -i 's/server_name .*/server_name _;/' /etc/nginx/sites-available/rag-frontend.conf
sudo ln -sf /etc/nginx/sites-available/rag-frontend.conf /etc/nginx/sites-enabled/rag-frontend.conf
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl enable --now nginx
sudo systemctl restart nginx
```

4. Verificación:

```bash
curl -I http://127.0.0.1
```

Acceso externo esperado:

```text
http://<FLOATING_IP>
```

## 8) Resumen operativo para futuras sesiones Codex

1. Entrar a FT3 con `tec_app2`.
2. Desde FT3, entrar a la VM con `ssh -i ~/.ssh/rag_cesga cesgaxuser@10.38.29.165`.
3. Sincronizar repo con `rsync` a `/opt/rag`.
4. Construir frontend (`scripts/build_frontend_bundle.sh`).
5. Publicar en `/var/www/rag-frontend` y reiniciar Nginx.
6. Validar con `curl -I http://127.0.0.1`.

Con este documento, tanto usuario como Codex pueden retomar el estado sin reconstruir la sesión.

## 9) Conexión VM → FT3: credenciales y configuración

### 9.1 Cómo funciona la conexión

La VM se conecta al FT3 usando **clave SSH** (sin contraseña). El flujo es:

```
VM (rag-prod-02)
  └─► SSH con clave ~/.ssh/rag_hpc
        └─► tec_app2@ft3.cesga.es
              └─► lanza `compute` en el supercomputador FT3
                    └─► rsync sync-back del índice FAISS y dataset a la VM
```

El script que orquesta esto es `scripts/compute_via_ft3.sh`.

### 9.2 Contraseña personal

**La contraseña personal de la cuenta FT3 no está guardada en ningún fichero del proyecto.**
La autenticación se hace exclusivamente por clave SSH privada (`rag_hpc`).

Sí hay una contraseña de base de datos guardada (usuario MySQL `backup_rt4` de RT HelpDesk), que no es la contraseña personal.

### 9.3 Fichero único de configuración — `state/daily_ingest.env`

Todo lo relativo a la cuenta FT3 y la conexión está en un único sitio:

```bash
# state/daily_ingest.env

export DAILY_INGEST_REMOTE_USER="tec_app2"          # usuario FT3
export DAILY_INGEST_REMOTE_HOST="ft3.cesga.es"      # host FT3
export DAILY_INGEST_REMOTE_SSH_KEY="/home/cesgaxuser/.ssh/rag_hpc"  # clave SSH privada
export DAILY_INGEST_REMOTE_WORKDIR="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag"

# BD de tickets RT (no es contraseña personal)
export DAILY_DB_USER="backup_rt4"
export DAILY_DB_PASSWORD="SolEnMuxia26#"
```

### 9.4 Guía completa para cambiar de cuenta FT3

Tras una revisión profunda del código, se han detectado referencias hardcoded al path `/home/cesga/tec_app2/`. Si se cambia de cuenta, hay que actualizar estos puntos:

**1. Configuración principal (Imprescindible)**
- `state/daily_ingest.env`: Cambiar `DAILY_INGEST_REMOTE_USER` y `DAILY_INGEST_REMOTE_WORKDIR`.

**2. Ficheros de Preprocesado (Configuración de entrada)**
- `config/preprocess.yaml`: El campo `input_path` tiene la ruta absoluta.
- `config/preprocess_passthrough.yaml`: Ídem.
- `config/preprocess_autoreply_only.yaml`: Ídem.

**3. Scripts de utilidad**
- `scripts/run_pipeline.sh`: Variable `INPUT` hardcoded.
- `scripts/compute_via_ft3.sh`: Tiene el path actual como valor por defecto (fallback).

**4. Scripts manuales en `state/`**
- Los scripts `state/manual_*.sh` (usados para re-indexados manuales) contienen comandos `cd` a la ruta absoluta de `tec_app2`.

**5. Pasos de autorización SSH**
- Generar (o reutilizar) la clave en la VM.
- Autorizar la clave pública en la nueva cuenta de FT3:
  ```bash
  ssh-copy-id -i ~/.ssh/rag_hpc.pub nueva_cuenta@ft3.cesga.es
  ```

**Resumen**: Aunque el cron diario se apoya en el `.env`, si el proyecto se mueve físicamente de `/home/cesga/tec_app2/` a otro sitio, el sistema fallará en los pasos de preprocesado y ejecución manual si no se revisan los ficheros YAML de la carpeta `config/`.

