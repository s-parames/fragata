const form = document.getElementById('search-form');
const queryEl = document.getElementById('query');
const languageSelectEl = document.getElementById('language-select');
const languageTriggerEl = document.getElementById('language-trigger');
const languageMenuEl = document.getElementById('language-menu');
const languageSelectorEl = document.getElementById('language-selector');
const departmentEl = document.getElementById('department');
const departmentSelectEl = document.getElementById('department-select');
const departmentTriggerEl = document.getElementById('department-trigger');
const departmentMenuEl = document.getElementById('department-menu');
const kEl = document.getElementById('k');
const dateFromEl = document.getElementById('date-from');
const dateToEl = document.getElementById('date-to');
const clearDatesBtn = document.getElementById('clear-dates-btn');
const statusEl = document.getElementById('status');
const resultsEl = document.getElementById('results');
const paginationEl = document.getElementById('pagination');
const workspaceTabSearchEl = document.getElementById('workspace-tab-search');
const workspaceTabCatalogEl = document.getElementById('workspace-tab-catalog');
const workspacePanelSearchEl = document.getElementById('workspace-panel-search');
const workspacePanelCatalogEl = document.getElementById('workspace-panel-catalog');
const catalogForm = document.getElementById('catalog-form');
const catalogFiltersToggleEl = document.getElementById('catalog-filters-toggle');
const catalogFiltersPanelEl = document.getElementById('catalog-filters-panel');
const catalogSourceTypeSelectEl = document.getElementById('catalog-source-type-select');
const catalogSourceTypeTriggerEl = document.getElementById('catalog-source-type-trigger');
const catalogSourceTypeMenuEl = document.getElementById('catalog-source-type-menu');
const catalogQueryEl = document.getElementById('catalog-q');
const catalogSourceTypeEl = document.getElementById('catalog-source-type');
const catalogDepartmentEl = document.getElementById('catalog-department');
const catalogDepartmentOptionsEl = document.getElementById('catalog-department-options');
const catalogApplyBtn = document.getElementById('catalog-apply-btn');
const catalogResetBtn = document.getElementById('catalog-reset-btn');
const catalogViewTabOverviewEl = document.getElementById('catalog-view-tab-overview');
const catalogViewTabBrowseEl = document.getElementById('catalog-view-tab-browse');
const catalogViewPanelOverviewEl = document.getElementById('catalog-view-panel-overview');
const catalogViewPanelBrowseEl = document.getElementById('catalog-view-panel-browse');
const catalogStatusEl = document.getElementById('catalog-status');
const catalogOverviewResultsEl = document.getElementById('catalog-overview-results');
const catalogResultsEl = document.getElementById('catalog-results');
const catalogPaginationEl = document.getElementById('catalog-pagination');
const catalogTotalChipEl = document.getElementById('catalog-total-chip');
const clearBtn = document.getElementById('clear-btn');
const searchBtn = document.getElementById('search-btn');
const ingestForm = document.getElementById('ingest-form');
const ingestSourceWebEl = document.getElementById('ingest-source-web');
const ingestSourcePdfEl = document.getElementById('ingest-source-pdf');
const ingestSourceRepoDocsEl = document.getElementById('ingest-source-repo-docs');
const ingestDepartmentEl = document.getElementById('ingest-department');
const ingestLabelEl = document.getElementById('ingest-label');
const ingestDepartmentOptionsEl = document.getElementById('ingest-department-options');
const ingestUrlEl = document.getElementById('ingest-url');
const ingestFileEl = document.getElementById('ingest-file');
const ingestWebFieldsEl = document.getElementById('ingest-web-fields');
const ingestPdfFieldsEl = document.getElementById('ingest-pdf-fields');
const ingestRepoDocsFieldsEl = document.getElementById('ingest-repo-docs-fields');
const ingestRepoUrlEl = document.getElementById('ingest-repo-url');
const ingestSubmitBtn = document.getElementById('ingest-submit-btn');
const ingestStatusEl = document.getElementById('ingest-status');
const ingestProgressBarEl = document.getElementById('ingest-progress-bar');
const ingestStageEl = document.getElementById('ingest-stage');
const ingestSummaryEl = document.getElementById('ingest-summary');
const purgeForm = document.getElementById('purge-form');
const purgeDepartmentEl = document.getElementById('purge-department');
const purgeDepartmentOptionsEl = document.getElementById('purge-department-options');
const purgeDryRunEl = document.getElementById('purge-dry-run');
const purgeConfirmEl = document.getElementById('purge-confirm');
const purgeSubmitBtn = document.getElementById('purge-submit-btn');
const purgeStatusEl = document.getElementById('purge-status');
const purgeProgressBarEl = document.getElementById('purge-progress-bar');
const purgeStageEl = document.getElementById('purge-stage');
const purgeSummaryEl = document.getElementById('purge-summary');
const adminToolsDisclosureEl = document.getElementById('admin-tools-disclosure');
const adminOpenCatalogBtn = document.getElementById('admin-open-catalog-btn');

const PAGE_SIZE = 25;
const CATALOG_PAGE_SIZE = 8;
const LANGUAGE_STORAGE_KEY = 'rag_ui_lang';
const MAX_CLIENT_PDF_UPLOAD_BYTES = 20 * 1024 * 1024;
const INGEST_DEPARTMENT_MIN_LENGTH = 3;
const INGEST_DEPARTMENT_MAX_LENGTH = 32;
const INGEST_LABEL_MAX_LENGTH = 120;
const INGEST_DEPARTMENT_UNSAFE_RE = /[^a-z0-9 _\-/]/;
const INGEST_DEPARTMENT_FINAL_RE = /^[a-z0-9][a-z0-9_-]{2,31}$/;
const INGEST_LABEL_RE = /^[A-Za-z0-9_.\- ]+$/;
const DEFAULT_SEARCH_DEPARTMENTS = Object.freeze(['all']);
const API_BASE_URL = resolveApiBaseUrl();

function normalizeApiBaseUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw.includes('{{') || raw.includes('}}') || raw.includes('%RAG_API_BASE_URL%')) return '';
  if (raw === 'null' || raw === 'undefined') return '';
  return raw.replace(/\/+$/, '');
}

function resolveApiBaseUrl() {
  const fromWindow = normalizeApiBaseUrl(window.__RAG_API_BASE_URL__);
  if (fromWindow) return fromWindow;
  const meta = document.querySelector('meta[name="rag-api-base-url"]');
  const fromMeta = normalizeApiBaseUrl(meta ? meta.getAttribute('content') : '');
  return fromMeta;
}

function apiUrl(path) {
  const raw = String(path || '').trim();
  if (!raw) return API_BASE_URL || '';
  const normalizedPath = raw.startsWith('/') ? raw : `/${raw}`;
  return API_BASE_URL ? `${API_BASE_URL}${normalizedPath}` : normalizedPath;
}

const I18N = Object.freeze({
  en: {
    'document.title': 'RAG Ticket Search',
    'meta.description': 'Search CESGA tickets with semantic + lexical retrieval, reranking, and filters.',
    'hero.eyebrow': 'Retrieval Assistant',
    'hero.title': 'RAG Ticket Search',
    'hero.subtitle': 'Search CESGA tickets with semantic + lexical retrieval, reranking, date filter and department filter.',
    'language.label': 'Language',
    'language.option.es': 'Castilian',
    'language.option.gl': 'Galician',
    'language.option.en': 'English',
    'form.queryLabel': 'Query',
    'form.queryPlaceholder': 'Optional for date-only mode. Example: cannot compile TDEP with gfortran -fpp -qopenmp',
    'form.departmentLabel': 'Department',
    'department.option.all': 'All',
    'department.option.aplicaciones': 'Applications',
    'department.option.sistemas': 'Systems',
    'department.option.bigdata': 'Bigdata',
    'department.option.slurm': 'Slurm',
    'form.topKLabel': 'Top K (0 = all in date-only mode)',
    'form.dateFromLabel': 'From',
    'form.dateToLabel': 'To',
    'form.noDateSelected': 'No date selected',
    'form.clearDates': 'Clear dates',
    'form.search': 'Search',
    'form.clear': 'Clear',
    'status.ready': 'Ready',
    'status.searching': 'Searching...',
    'status.done': 'Done. {total} results.',
    'status.dateCleared': 'Date filters cleared',
    'validation.kRange': 'Top K must be an integer between 0 and 20.',
    'validation.queryOrDate': 'Provide a query or at least one date filter.',
    'validation.kZeroDateOnly': 'Top K = 0 is only allowed for date-only searches.',
    'validation.invalidRange': 'Invalid date range: "From" must be before "To".',
    'validation.departmentFilterInvalid': 'Invalid department filter. Use 3-32 chars: letters, numbers, "_" or "-".',
    'error.engineWarmup': 'Engine is warming up (first load). Retry in a few seconds.',
    'error.prefix': 'Error: {message}',
    'error.httpStatus': 'HTTP {status}',
    'results.none': 'No results found.',
    'results.sectionTitle': 'Results',
    'results.summary': 'Showing {start}-{end} of {total}',
    'workspace.tab.search': 'Results',
    'workspace.tab.catalog': 'Catalog',
    'admin.kicker': 'Knowledge ops',
    'admin.toggle': 'Add new files and manage data',
    'admin.title': 'Knowledge intake',
    'admin.subtitle': 'Ingestion, purge, and catalog access are hidden here until you open this section.',
    'admin.openCatalog': 'Open catalog',
    'pagination.prev': 'Prev',
    'pagination.next': 'Next',
    'card.openTicket': 'open ticket',
    'card.openWeb': 'open web',
    'card.openDocument': 'open document',
    'card.noDate': 'no date',
    'card.rerank': 'rerank',
    'card.fused': 'fused',
    'card.conversation': 'conversation',
    'card.ticket': 'ticket',
    'card.chunk': 'chunk',
    'card.lastUpdate': 'last update',
    'card.department': 'dept',
    'ingest.title': 'Ingestion',
    'ingest.subtitle': 'Submit website, repository docs, or PDF jobs and track processing.',
    'ingest.sourceTypeLabel': 'Source type',
    'ingest.sourceType.web': 'Website URL',
    'ingest.sourceType.pdf': 'PDF upload',
    'ingest.sourceType.repoDocs': 'Repo docs URL',
    'ingest.departmentLabel': 'Department',
    'ingest.ingestLabelLabel': 'Ingest label',
    'ingest.ingestLabelPlaceholder': 'optional batch label',
    'ingest.departmentPlaceholder': 'Select or type department (example: sistemas or data_science)',
    'ingest.urlLabel': 'Website URL',
    'ingest.urlPlaceholder': 'https://example.com/docs/',
    'ingest.fileLabel': 'PDF file',
    'ingest.repoDocsLabel': 'Repository docs URL',
    'ingest.repoDocsPlaceholder': 'https://github.com/owner/repo/blob/main/README.md',
    'ingest.repoDocsHelp': 'Supports public GitHub/GitLab README.md and wiki URLs.',
    'ingest.repoDocsExamples': 'Examples',
    'ingest.submit': 'Start ingestion',
    'ingest.status.idle': 'No active ingestion job.',
    'ingest.status.submitting': 'Submitting ingestion job...',
    'ingest.status.queued': 'Job {jobId} queued.',
    'ingest.status.running': 'Job {jobId} running.',
    'ingest.status.succeeded': 'Job {jobId} completed.',
    'ingest.status.failed': 'Job {jobId} failed.',
    'ingest.validation.departmentRequired': 'Department is required.',
    'ingest.validation.departmentInvalid': 'Use 3-32 chars: letters, numbers, spaces, "_" or "-".',
    'ingest.validation.ingestLabelInvalid': 'Ingest label must use 1-120 letters, numbers, spaces, ".", "_" or "-".',
    'ingest.validation.urlRequired': 'A valid public HTTP/HTTPS URL is required.',
    'ingest.validation.repoDocsUrlRequired': 'A valid public GitHub/GitLab README.md or wiki URL is required.',
    'ingest.validation.pdfRequired': 'Select a PDF file to upload.',
    'ingest.validation.pdfInvalidType': 'Invalid file type. Upload a PDF file.',
    'ingest.validation.pdfTooLarge': 'PDF exceeds {maxMB} MB client-side limit.',
    'ingest.validation.jobActive': 'A job is already running ({jobId}). Wait for completion.',
    'ingest.error.poll': 'Could not refresh job status: {message}',
    'ingest.error.jobNotFound': 'Ingestion job {jobId} was not found.',
    'ingest.stageLabel': 'Stage',
    'ingest.stage.queued': 'queued',
    'ingest.stage.dispatch': 'dispatch',
    'ingest.stage.resource_requested': 'resource request',
    'ingest.stage.waiting_resources': 'waiting resources',
    'ingest.stage.running_remote': 'running remote',
    'ingest.stage.sync_back': 'sync back',
    'ingest.stage.acquire_source': 'acquire source',
    'ingest.stage.extract': 'extract',
    'ingest.stage.prepare': 'prepare',
    'ingest.stage.chunk': 'chunk',
    'ingest.stage.merge': 'merge',
    'ingest.stage.index_append': 'index append',
    'ingest.stage.reload': 'reload engine',
    'ingest.stage.upload': 'upload',
    'ingest.stage.failed': 'failed',
    'ingest.summary.jobId': 'Job ID',
    'ingest.summary.state': 'State',
    'ingest.summary.message': 'Message',
    'ingest.summary.chunkRows': 'Chunk rows',
    'ingest.summary.deltaRows': 'Delta rows',
    'ingest.summary.mergedRows': 'Merged rows',
    'ingest.summary.indexUpdated': 'Index updated',
    'ingest.summary.datasetPath': 'Dataset output',
    'ingest.summary.deltaPath': 'Delta output',
    'ingest.summary.mergeSummaryPath': 'Merge summary',
    'ingest.summary.appendSummaryPath': 'Append summary',
    'ingest.summary.indexPath': 'Index output',
    'ingest.summary.reloadGeneration': 'Reload generation',
    'ingest.summary.reloadLoadedAt': 'Reload loaded at',
    'ingest.summary.error': 'Error',
    'ingest.state.queued': 'queued',
    'ingest.state.running': 'running',
    'ingest.state.succeeded': 'succeeded',
    'ingest.state.failed': 'failed',
    'purge.title': 'Department Purge',
    'purge.subtitle': 'Remove one department from dataset, rebuild index, and reload engine.',
    'purge.departmentLabel': 'Department',
    'purge.departmentPlaceholder': 'Select or type department to purge (example: sistemas)',
    'purge.dryRunLabel': 'Dry-run (show impact only, do not write dataset)',
    'purge.confirmLabel': 'I confirm this purge request and full index rebuild.',
    'purge.warning': 'Live mode rewrites the global dataset and FAISS index.',
    'purge.submit': 'Start purge',
    'purge.status.idle': 'No active purge job.',
    'purge.status.submitting': 'Submitting purge job...',
    'purge.status.queued': 'Purge job {jobId} queued.',
    'purge.status.running': 'Purge job {jobId} running.',
    'purge.status.succeeded': 'Purge job {jobId} completed.',
    'purge.status.failed': 'Purge job {jobId} failed.',
    'purge.validation.departmentRequired': 'Department is required.',
    'purge.validation.departmentInvalid': 'Use 3-32 chars: letters, numbers, spaces, "_" or "-".',
    'purge.validation.confirmRequired': 'Explicit confirmation is required before purge.',
    'purge.validation.jobActive': 'A purge job is already running ({jobId}). Wait for completion.',
    'purge.validation.cancelled': 'Purge request cancelled before submission.',
    'purge.error.poll': 'Could not refresh purge job status: {message}',
    'purge.error.jobNotFound': 'Purge job {jobId} was not found.',
    'purge.stageLabel': 'Stage',
    'purge.stage.queued': 'queued',
    'purge.stage.dispatch': 'dispatch',
    'purge.stage.purge_dataset': 'purge dataset',
    'purge.stage.full_rebuild': 'full rebuild',
    'purge.stage.reload': 'reload engine',
    'purge.stage.failed': 'failed',
    'purge.summary.jobId': 'Job ID',
    'purge.summary.state': 'State',
    'purge.summary.message': 'Message',
    'purge.summary.targetDepartment': 'Target department',
    'purge.summary.dryRun': 'Dry run',
    'purge.summary.rowsBefore': 'Rows before',
    'purge.summary.rowsRemoved': 'Rows removed',
    'purge.summary.rowsAfter': 'Rows after',
    'purge.summary.datasetPath': 'Dataset output',
    'purge.summary.purgeSummaryPath': 'Purge summary',
    'purge.summary.backupDatasetPath': 'Backup dataset',
    'purge.summary.rebuildStatus': 'Rebuild status',
    'purge.summary.rebuildDuration': 'Rebuild duration (s)',
    'purge.summary.rebuildIndexPath': 'Rebuild index path',
    'purge.summary.rebuildVectorCount': 'Rebuild vector count',
    'purge.summary.rebuildDocCount': 'Rebuild doc count',
    'purge.summary.rebuildSummaryPath': 'Rebuild summary',
    'purge.summary.reloadGeneration': 'Reload generation',
    'purge.summary.reloadLoadedAt': 'Reload loaded at',
    'purge.summary.error': 'Error',
    'purge.state.queued': 'queued',
    'purge.state.running': 'running',
    'purge.state.succeeded': 'succeeded',
    'purge.state.failed': 'failed',
    'purge.confirm.dialog': 'Confirm purge for "{department}"? This will run purge, full rebuild, and reload.',
    'purge.confirm.dialogDryRun': 'Confirm dry-run for "{department}"? No dataset changes will be written.',
    'panel.searchResults': 'Search Workspace',
    'panel.inventory': 'Knowledge Inventory',
    'catalog.title': 'RAG Catalog',
    'catalog.subtitle': 'Browse websites, PDFs, and ticket sources already represented in the dataset.',
    'catalog.summary.total': '{total} sources',
    'catalog.filters.kicker': 'Catalog tools',
    'catalog.filters.toggle': 'Search and filters',
    'catalog.filters.toggleAriaExpand': 'Open search and filters',
    'catalog.filters.toggleAriaCollapse': 'Close search and filters',
    'catalog.form.searchLabel': 'Search catalog',
    'catalog.form.searchPlaceholder': 'Title, host, source URL, or ingest job',
    'catalog.form.sourceTypeLabel': 'Source type',
    'catalog.form.departmentLabel': 'Department',
    'catalog.form.departmentPlaceholder': 'All departments or one team',
    'catalog.form.submit': 'Apply filters',
    'catalog.form.reset': 'Reset',
    'catalog.view.overview': 'Source list',
    'catalog.view.browse': 'Source cards',
    'catalog.type.all': 'All sources',
    'catalog.type.ticket': 'Tickets',
    'catalog.type.web': 'Websites',
    'catalog.type.pdf': 'PDFs',
    'catalog.status.loading': 'Loading catalog...',
    'catalog.status.loaded': '{total} sources available.',
    'catalog.status.error': 'Catalog error: {message}',
    'catalog.empty': 'No catalog entries match the current filters.',
    'catalog.meta.source': 'source',
    'catalog.meta.host': 'host',
    'catalog.meta.chunks': 'chunks',
    'catalog.meta.lastUpdated': 'last update',
    'catalog.meta.ingestedAt': 'ingested',
    'catalog.meta.jobId': 'job',
    'catalog.overview.kicker': 'Source map',
    'catalog.overview.subtitle': 'Open one source group and inspect the pages, PDFs, or tickets currently inside it.',
    'catalog.overview.loading': 'Loading source list...',
    'catalog.overview.empty': 'No source groups match the current filters.',
    'catalog.overview.documents': '{count} items',
    'catalog.overview.departments': '{count} departments',
    'catalog.overview.groupCount': '{count} groups',
    'catalog.overview.pages': '{count} pages',
    'catalog.overview.pdfs': '{count} PDFs',
    'catalog.overview.tickets': '{count} tickets',
    'catalog.overview.open': 'Open',
    'catalog.overview.noPath': 'No path',
    'catalog.link.open': 'open source',
    'catalog.validation.departmentInvalid': 'Invalid department filter. Use 3-32 chars: letters, numbers, "_" or "-".',
    'common.yes': 'yes',
    'common.no': 'no',
  },
  es: {
    'document.title': 'Buscador de Tickets RAG',
    'meta.description': 'Busca tickets de CESGA con recuperacion semantica + lexica, reranking y filtros.',
    'hero.eyebrow': 'Asistente de Recuperacion',
    'hero.title': 'Buscador de Tickets RAG',
    'hero.subtitle': 'Busca tickets de CESGA con recuperacion semantica + lexica, reranking, filtro de fechas y filtro por departamento.',
    'language.label': 'Idioma',
    'language.option.es': 'Castellano',
    'language.option.gl': 'Galego',
    'language.option.en': 'English',
    'form.queryLabel': 'Consulta',
    'form.queryPlaceholder': 'Opcional para modo solo fechas. Ejemplo: cannot compile TDEP with gfortran -fpp -qopenmp',
    'form.departmentLabel': 'Departamento',
    'department.option.all': 'Todos',
    'department.option.aplicaciones': 'Aplicaciones',
    'department.option.sistemas': 'Sistemas',
    'department.option.bigdata': 'Bigdata',
    'department.option.slurm': 'Slurm',
    'form.topKLabel': 'Top K (0 = todos en modo solo fechas)',
    'form.dateFromLabel': 'Desde',
    'form.dateToLabel': 'Hasta',
    'form.noDateSelected': 'Sin fecha seleccionada',
    'form.clearDates': 'Limpiar fechas',
    'form.search': 'Buscar',
    'form.clear': 'Limpiar',
    'status.ready': 'Listo',
    'status.searching': 'Buscando...',
    'status.done': 'Hecho. {total} resultados.',
    'status.dateCleared': 'Filtros de fecha limpiados',
    'validation.kRange': 'Top K debe ser un entero entre 0 y 20.',
    'validation.queryOrDate': 'Introduce una consulta o al menos un filtro de fecha.',
    'validation.kZeroDateOnly': 'Top K = 0 solo esta permitido en busquedas sin consulta.',
    'validation.invalidRange': 'Rango de fechas invalido: "Desde" debe ser anterior a "Hasta".',
    'validation.departmentFilterInvalid': 'Filtro de departamento invalido. Usa 3-32 caracteres: letras, numeros, "_" o "-".',
    'error.engineWarmup': 'El motor se esta iniciando (primera carga). Reintenta en unos segundos.',
    'error.prefix': 'Error: {message}',
    'error.httpStatus': 'HTTP {status}',
    'results.none': 'No se han encontrado resultados.',
    'results.sectionTitle': 'Resultados',
    'results.summary': 'Mostrando {start}-{end} de {total}',
    'workspace.tab.search': 'Resultados',
    'workspace.tab.catalog': 'Catalogo',
    'admin.kicker': 'Gestion de conocimiento',
    'admin.toggle': 'Anadir nuevos archivos y gestionar datos',
    'admin.title': 'Entrada de nueva informacion',
    'admin.subtitle': 'Aqui tienes ingestion, purge y acceso al catalogo ocultos hasta que abras esta seccion.',
    'admin.openCatalog': 'Abrir catalogo',
    'pagination.prev': 'Anterior',
    'pagination.next': 'Siguiente',
    'card.openTicket': 'abrir ticket',
    'card.openWeb': 'abrir web',
    'card.openDocument': 'abrir documento',
    'card.noDate': 'sin fecha',
    'card.rerank': 'rerank',
    'card.fused': 'fusionado',
    'card.conversation': 'conversacion',
    'card.ticket': 'ticket',
    'card.chunk': 'fragmento',
    'card.lastUpdate': 'ultima actualizacion',
    'card.department': 'depto',
    'ingest.title': 'Ingestion',
    'ingest.subtitle': 'Envia trabajos web, de documentacion de repositorio o PDF y sigue su progreso.',
    'ingest.sourceTypeLabel': 'Tipo de fuente',
    'ingest.sourceType.web': 'URL web',
    'ingest.sourceType.pdf': 'Subir PDF',
    'ingest.sourceType.repoDocs': 'URL repo docs',
    'ingest.departmentLabel': 'Departamento',
    'ingest.ingestLabelLabel': 'Etiqueta de ingesta',
    'ingest.ingestLabelPlaceholder': 'etiqueta opcional del lote',
    'ingest.departmentPlaceholder': 'Selecciona o escribe departamento (ejemplo: sistemas o data_science)',
    'ingest.urlLabel': 'URL web',
    'ingest.urlPlaceholder': 'https://example.com/docs/',
    'ingest.fileLabel': 'Archivo PDF',
    'ingest.repoDocsLabel': 'URL de documentacion del repositorio',
    'ingest.repoDocsPlaceholder': 'https://github.com/owner/repo/blob/main/README.md',
    'ingest.repoDocsHelp': 'Admite URLs publicas de README.md y wiki de GitHub/GitLab.',
    'ingest.repoDocsExamples': 'Ejemplos',
    'ingest.submit': 'Iniciar ingestion',
    'ingest.status.idle': 'No hay un trabajo de ingestion activo.',
    'ingest.status.submitting': 'Enviando trabajo de ingestion...',
    'ingest.status.queued': 'Trabajo {jobId} en cola.',
    'ingest.status.running': 'Trabajo {jobId} en ejecucion.',
    'ingest.status.succeeded': 'Trabajo {jobId} completado.',
    'ingest.status.failed': 'Trabajo {jobId} con error.',
    'ingest.validation.departmentRequired': 'El departamento es obligatorio.',
    'ingest.validation.departmentInvalid': 'Usa 3-32 caracteres: letras, numeros, espacios, "_" o "-".',
    'ingest.validation.ingestLabelInvalid': 'La etiqueta de ingesta debe usar 1-120 caracteres: letras, numeros, espacios, ".", "_" o "-".',
    'ingest.validation.urlRequired': 'Se necesita una URL publica HTTP/HTTPS valida.',
    'ingest.validation.repoDocsUrlRequired': 'Se necesita una URL publica valida de README.md o wiki de GitHub/GitLab.',
    'ingest.validation.pdfRequired': 'Selecciona un archivo PDF para subir.',
    'ingest.validation.pdfInvalidType': 'Tipo de archivo invalido. Sube un PDF.',
    'ingest.validation.pdfTooLarge': 'El PDF supera el limite cliente de {maxMB} MB.',
    'ingest.validation.jobActive': 'Ya hay un trabajo en ejecucion ({jobId}). Espera a que termine.',
    'ingest.error.poll': 'No se pudo refrescar el estado del trabajo: {message}',
    'ingest.error.jobNotFound': 'No se encontro el trabajo de ingestion {jobId}.',
    'ingest.stageLabel': 'Fase',
    'ingest.stage.queued': 'en cola',
    'ingest.stage.dispatch': 'despacho',
    'ingest.stage.resource_requested': 'solicitud recursos',
    'ingest.stage.waiting_resources': 'espera recursos',
    'ingest.stage.running_remote': 'ejecucion remota',
    'ingest.stage.sync_back': 'sincronizacion',
    'ingest.stage.acquire_source': 'adquisicion',
    'ingest.stage.extract': 'extraccion',
    'ingest.stage.prepare': 'preparacion',
    'ingest.stage.chunk': 'chunking',
    'ingest.stage.merge': 'merge',
    'ingest.stage.index_append': 'append indice',
    'ingest.stage.reload': 'recarga motor',
    'ingest.stage.upload': 'subida',
    'ingest.stage.failed': 'fallido',
    'ingest.summary.jobId': 'Job ID',
    'ingest.summary.state': 'Estado',
    'ingest.summary.message': 'Mensaje',
    'ingest.summary.chunkRows': 'Filas chunk',
    'ingest.summary.deltaRows': 'Filas delta',
    'ingest.summary.mergedRows': 'Filas merge',
    'ingest.summary.indexUpdated': 'Indice actualizado',
    'ingest.summary.datasetPath': 'Salida dataset',
    'ingest.summary.deltaPath': 'Salida delta',
    'ingest.summary.mergeSummaryPath': 'Resumen merge',
    'ingest.summary.appendSummaryPath': 'Resumen append',
    'ingest.summary.indexPath': 'Salida indice',
    'ingest.summary.reloadGeneration': 'Generacion recarga',
    'ingest.summary.reloadLoadedAt': 'Recargado en',
    'ingest.summary.error': 'Error',
    'ingest.state.queued': 'en cola',
    'ingest.state.running': 'en ejecucion',
    'ingest.state.succeeded': 'completado',
    'ingest.state.failed': 'fallido',
    'purge.title': 'Purgado por departamento',
    'purge.subtitle': 'Elimina un departamento del dataset, reconstruye indice y recarga motor.',
    'purge.departmentLabel': 'Departamento',
    'purge.departmentPlaceholder': 'Selecciona o escribe departamento a purgar (ejemplo: sistemas)',
    'purge.dryRunLabel': 'Dry-run (solo impacto, sin escribir dataset)',
    'purge.confirmLabel': 'Confirmo esta solicitud de purge y reconstruccion completa del indice.',
    'purge.warning': 'El modo real reescribe dataset global e indice FAISS.',
    'purge.submit': 'Iniciar purge',
    'purge.status.idle': 'No hay un trabajo de purge activo.',
    'purge.status.submitting': 'Enviando trabajo de purge...',
    'purge.status.queued': 'Trabajo de purge {jobId} en cola.',
    'purge.status.running': 'Trabajo de purge {jobId} en ejecucion.',
    'purge.status.succeeded': 'Trabajo de purge {jobId} completado.',
    'purge.status.failed': 'Trabajo de purge {jobId} con error.',
    'purge.validation.departmentRequired': 'El departamento es obligatorio.',
    'purge.validation.departmentInvalid': 'Usa 3-32 caracteres: letras, numeros, espacios, "_" o "-".',
    'purge.validation.confirmRequired': 'Se requiere confirmacion explicita antes del purge.',
    'purge.validation.jobActive': 'Ya hay un trabajo de purge en ejecucion ({jobId}). Espera a que termine.',
    'purge.validation.cancelled': 'Solicitud de purge cancelada antes del envio.',
    'purge.error.poll': 'No se pudo refrescar el estado del purge: {message}',
    'purge.error.jobNotFound': 'No se encontro el trabajo de purge {jobId}.',
    'purge.stageLabel': 'Fase',
    'purge.stage.queued': 'en cola',
    'purge.stage.dispatch': 'despacho',
    'purge.stage.purge_dataset': 'purge dataset',
    'purge.stage.full_rebuild': 'reconstruccion completa',
    'purge.stage.reload': 'recarga motor',
    'purge.stage.failed': 'fallido',
    'purge.summary.jobId': 'Job ID',
    'purge.summary.state': 'Estado',
    'purge.summary.message': 'Mensaje',
    'purge.summary.targetDepartment': 'Departamento objetivo',
    'purge.summary.dryRun': 'Dry run',
    'purge.summary.rowsBefore': 'Filas antes',
    'purge.summary.rowsRemoved': 'Filas eliminadas',
    'purge.summary.rowsAfter': 'Filas despues',
    'purge.summary.datasetPath': 'Salida dataset',
    'purge.summary.purgeSummaryPath': 'Resumen purge',
    'purge.summary.backupDatasetPath': 'Backup dataset',
    'purge.summary.rebuildStatus': 'Estado rebuild',
    'purge.summary.rebuildDuration': 'Duracion rebuild (s)',
    'purge.summary.rebuildIndexPath': 'Ruta indice rebuild',
    'purge.summary.rebuildVectorCount': 'Conteo vectores rebuild',
    'purge.summary.rebuildDocCount': 'Conteo docs rebuild',
    'purge.summary.rebuildSummaryPath': 'Resumen rebuild',
    'purge.summary.reloadGeneration': 'Generacion recarga',
    'purge.summary.reloadLoadedAt': 'Recargado en',
    'purge.summary.error': 'Error',
    'purge.state.queued': 'en cola',
    'purge.state.running': 'en ejecucion',
    'purge.state.succeeded': 'completado',
    'purge.state.failed': 'fallido',
    'purge.confirm.dialog': 'Confirmar purge para "{department}"? Ejecutara purge, rebuild completo y recarga.',
    'purge.confirm.dialogDryRun': 'Confirmar dry-run para "{department}"? No se escribiran cambios en dataset.',
    'panel.searchResults': 'Espacio de busqueda',
    'panel.inventory': 'Inventario de conocimiento',
    'catalog.title': 'Catalogo RAG',
    'catalog.subtitle': 'Explora webs, PDFs y fuentes de tickets ya representadas en el dataset.',
    'catalog.summary.total': '{total} fuentes',
    'catalog.filters.kicker': 'Herramientas del catalogo',
    'catalog.filters.toggle': 'Busqueda y filtros',
    'catalog.filters.toggleAriaExpand': 'Abrir busqueda y filtros',
    'catalog.filters.toggleAriaCollapse': 'Cerrar busqueda y filtros',
    'catalog.form.searchLabel': 'Buscar en el catalogo',
    'catalog.form.searchPlaceholder': 'Titulo, host, URL de origen o job de ingesta',
    'catalog.form.sourceTypeLabel': 'Tipo de fuente',
    'catalog.form.departmentLabel': 'Departamento',
    'catalog.form.departmentPlaceholder': 'Todos los departamentos o un equipo',
    'catalog.form.submit': 'Aplicar filtros',
    'catalog.form.reset': 'Restablecer',
    'catalog.view.overview': 'Lista de fuentes',
    'catalog.view.browse': 'Tarjetas',
    'catalog.type.all': 'Todas las fuentes',
    'catalog.type.ticket': 'Tickets',
    'catalog.type.web': 'Webs',
    'catalog.type.pdf': 'PDFs',
    'catalog.status.loading': 'Cargando catalogo...',
    'catalog.status.loaded': '{total} fuentes disponibles.',
    'catalog.status.error': 'Error de catalogo: {message}',
    'catalog.empty': 'No hay entradas del catalogo para los filtros actuales.',
    'catalog.meta.source': 'fuente',
    'catalog.meta.host': 'host',
    'catalog.meta.chunks': 'chunks',
    'catalog.meta.lastUpdated': 'ultima actualizacion',
    'catalog.meta.ingestedAt': 'ingestado',
    'catalog.meta.jobId': 'job',
    'catalog.overview.kicker': 'Mapa de fuentes',
    'catalog.overview.subtitle': 'Abre un grupo y revisa las paginas, PDFs o tickets que contiene ahora mismo.',
    'catalog.overview.loading': 'Cargando lista de fuentes...',
    'catalog.overview.empty': 'No hay grupos de fuentes para los filtros actuales.',
    'catalog.overview.documents': '{count} elementos',
    'catalog.overview.departments': '{count} departamentos',
    'catalog.overview.groupCount': '{count} grupos',
    'catalog.overview.pages': '{count} paginas',
    'catalog.overview.pdfs': '{count} PDFs',
    'catalog.overview.tickets': '{count} tickets',
    'catalog.overview.open': 'Abrir',
    'catalog.overview.noPath': 'Sin ruta',
    'catalog.link.open': 'abrir fuente',
    'catalog.validation.departmentInvalid': 'Filtro de departamento invalido. Usa 3-32 caracteres: letras, numeros, "_" o "-".',
    'common.yes': 'si',
    'common.no': 'no',
  },
  gl: {
    'document.title': 'Buscador de Tickets RAG',
    'meta.description': 'Busca tickets de CESGA con recuperacion semantica + lexica, reranking e filtros.',
    'hero.eyebrow': 'Asistente de Recuperacion',
    'hero.title': 'Buscador de Tickets RAG',
    'hero.subtitle': 'Busca tickets de CESGA con recuperacion semantica + lexica, reranking, filtro de datas e filtro por departamento.',
    'language.label': 'Idioma',
    'language.option.es': 'Castellano',
    'language.option.gl': 'Galego',
    'language.option.en': 'English',
    'form.queryLabel': 'Consulta',
    'form.queryPlaceholder': 'Opcional para modo so datas. Exemplo: cannot compile TDEP with gfortran -fpp -qopenmp',
    'form.departmentLabel': 'Departamento',
    'department.option.all': 'Todos',
    'department.option.aplicaciones': 'Aplicacions',
    'department.option.sistemas': 'Sistemas',
    'department.option.bigdata': 'Bigdata',
    'department.option.slurm': 'Slurm',
    'form.topKLabel': 'Top K (0 = todos no modo so datas)',
    'form.dateFromLabel': 'Desde',
    'form.dateToLabel': 'Ata',
    'form.noDateSelected': 'Sen data seleccionada',
    'form.clearDates': 'Limpar datas',
    'form.search': 'Buscar',
    'form.clear': 'Limpar',
    'status.ready': 'Preparado',
    'status.searching': 'Buscando...',
    'status.done': 'Feito. {total} resultados.',
    'status.dateCleared': 'Filtros de data limpos',
    'validation.kRange': 'Top K debe ser un enteiro entre 0 e 20.',
    'validation.queryOrDate': 'Introduce unha consulta ou polo menos un filtro de data.',
    'validation.kZeroDateOnly': 'Top K = 0 so esta permitido en buscas sen consulta.',
    'validation.invalidRange': 'Rango de datas invalido: "Desde" debe ser anterior a "Ata".',
    'validation.departmentFilterInvalid': 'Filtro de departamento invalido. Usa 3-32 caracteres: letras, numeros, "_" ou "-".',
    'error.engineWarmup': 'O motor esta iniciando (primeira carga). Reintenta en uns segundos.',
    'error.prefix': 'Erro: {message}',
    'error.httpStatus': 'HTTP {status}',
    'results.none': 'Non se atoparon resultados.',
    'results.sectionTitle': 'Resultados',
    'results.summary': 'Mostrando {start}-{end} de {total}',
    'workspace.tab.search': 'Resultados',
    'workspace.tab.catalog': 'Catalogo',
    'admin.kicker': 'Xestion de conecemento',
    'admin.toggle': 'Engadir novos ficheiros e xestionar datos',
    'admin.title': 'Entrada de nova informacion',
    'admin.subtitle': 'Aqui tes ingestion, purge e acceso ao catalogo ocultos ata que abras esta seccion.',
    'admin.openCatalog': 'Abrir catalogo',
    'pagination.prev': 'Anterior',
    'pagination.next': 'Seguinte',
    'card.openTicket': 'abrir ticket',
    'card.openWeb': 'abrir web',
    'card.openDocument': 'abrir documento',
    'card.noDate': 'sen data',
    'card.rerank': 'rerank',
    'card.fused': 'fusionado',
    'card.conversation': 'conversa',
    'card.ticket': 'ticket',
    'card.chunk': 'fragmento',
    'card.lastUpdate': 'ultima actualizacion',
    'card.department': 'depto',
    'ingest.title': 'Ingestion',
    'ingest.subtitle': 'Envia traballos web, de documentacion de repositorio ou PDF e segue o progreso.',
    'ingest.sourceTypeLabel': 'Tipo de fonte',
    'ingest.sourceType.web': 'URL web',
    'ingest.sourceType.pdf': 'Subida PDF',
    'ingest.sourceType.repoDocs': 'URL repo docs',
    'ingest.departmentLabel': 'Departamento',
    'ingest.ingestLabelLabel': 'Etiqueta de inxesta',
    'ingest.ingestLabelPlaceholder': 'etiqueta opcional do lote',
    'ingest.departmentPlaceholder': 'Selecciona ou escribe departamento (exemplo: sistemas ou data_science)',
    'ingest.urlLabel': 'URL web',
    'ingest.urlPlaceholder': 'https://example.com/docs/',
    'ingest.fileLabel': 'Ficheiro PDF',
    'ingest.repoDocsLabel': 'URL de documentacion do repositorio',
    'ingest.repoDocsPlaceholder': 'https://github.com/owner/repo/blob/main/README.md',
    'ingest.repoDocsHelp': 'Admite URLs publicas de README.md e wiki de GitHub/GitLab.',
    'ingest.repoDocsExamples': 'Exemplos',
    'ingest.submit': 'Iniciar ingestion',
    'ingest.status.idle': 'Non hai un traballo de ingestion activo.',
    'ingest.status.submitting': 'Enviando traballo de ingestion...',
    'ingest.status.queued': 'Traballo {jobId} en cola.',
    'ingest.status.running': 'Traballo {jobId} en execucion.',
    'ingest.status.succeeded': 'Traballo {jobId} completado.',
    'ingest.status.failed': 'Traballo {jobId} con erro.',
    'ingest.validation.departmentRequired': 'O departamento e obrigatorio.',
    'ingest.validation.departmentInvalid': 'Usa 3-32 caracteres: letras, numeros, espazos, "_" ou "-".',
    'ingest.validation.ingestLabelInvalid': 'A etiqueta de inxesta debe usar 1-120 caracteres: letras, numeros, espazos, ".", "_" ou "-".',
    'ingest.validation.urlRequired': 'Necesitase unha URL publica HTTP/HTTPS valida.',
    'ingest.validation.repoDocsUrlRequired': 'Necesitase unha URL publica valida de README.md ou wiki de GitHub/GitLab.',
    'ingest.validation.pdfRequired': 'Selecciona un ficheiro PDF para subir.',
    'ingest.validation.pdfInvalidType': 'Tipo de ficheiro invalido. Sube un PDF.',
    'ingest.validation.pdfTooLarge': 'O PDF supera o limite cliente de {maxMB} MB.',
    'ingest.validation.jobActive': 'Xa hai un traballo en execucion ({jobId}). Agarda a finalizacion.',
    'ingest.error.poll': 'Non se puido refrescar o estado do traballo: {message}',
    'ingest.error.jobNotFound': 'Non se atopou o traballo de ingestion {jobId}.',
    'ingest.stageLabel': 'Fase',
    'ingest.stage.queued': 'en cola',
    'ingest.stage.dispatch': 'despacho',
    'ingest.stage.resource_requested': 'solicitude recursos',
    'ingest.stage.waiting_resources': 'agarda recursos',
    'ingest.stage.running_remote': 'execucion remota',
    'ingest.stage.sync_back': 'sincronizacion',
    'ingest.stage.acquire_source': 'adquisicion',
    'ingest.stage.extract': 'extraccion',
    'ingest.stage.prepare': 'preparacion',
    'ingest.stage.chunk': 'chunking',
    'ingest.stage.merge': 'merge',
    'ingest.stage.index_append': 'append indice',
    'ingest.stage.reload': 'recarga motor',
    'ingest.stage.upload': 'subida',
    'ingest.stage.failed': 'fallido',
    'ingest.summary.jobId': 'Job ID',
    'ingest.summary.state': 'Estado',
    'ingest.summary.message': 'Mensaxe',
    'ingest.summary.chunkRows': 'Filas chunk',
    'ingest.summary.deltaRows': 'Filas delta',
    'ingest.summary.mergedRows': 'Filas merge',
    'ingest.summary.indexUpdated': 'Indice actualizado',
    'ingest.summary.datasetPath': 'Saida dataset',
    'ingest.summary.deltaPath': 'Saida delta',
    'ingest.summary.mergeSummaryPath': 'Resumo merge',
    'ingest.summary.appendSummaryPath': 'Resumo append',
    'ingest.summary.indexPath': 'Saida indice',
    'ingest.summary.reloadGeneration': 'Xeracion recarga',
    'ingest.summary.reloadLoadedAt': 'Recargado en',
    'ingest.summary.error': 'Erro',
    'ingest.state.queued': 'en cola',
    'ingest.state.running': 'en execucion',
    'ingest.state.succeeded': 'completado',
    'ingest.state.failed': 'fallido',
    'purge.title': 'Purgado por departamento',
    'purge.subtitle': 'Elimina un departamento do dataset, reconstrue indice e recarga motor.',
    'purge.departmentLabel': 'Departamento',
    'purge.departmentPlaceholder': 'Selecciona ou escribe departamento a purgar (exemplo: sistemas)',
    'purge.dryRunLabel': 'Dry-run (so impacto, sen escribir dataset)',
    'purge.confirmLabel': 'Confirmo esta solicitude de purge e reconstrucion completa do indice.',
    'purge.warning': 'O modo real reescribe dataset global e indice FAISS.',
    'purge.submit': 'Iniciar purge',
    'purge.status.idle': 'Non hai un traballo de purge activo.',
    'purge.status.submitting': 'Enviando traballo de purge...',
    'purge.status.queued': 'Traballo de purge {jobId} en cola.',
    'purge.status.running': 'Traballo de purge {jobId} en execucion.',
    'purge.status.succeeded': 'Traballo de purge {jobId} completado.',
    'purge.status.failed': 'Traballo de purge {jobId} con erro.',
    'purge.validation.departmentRequired': 'O departamento e obrigatorio.',
    'purge.validation.departmentInvalid': 'Usa 3-32 caracteres: letras, numeros, espazos, "_" ou "-".',
    'purge.validation.confirmRequired': 'Requirase confirmacion explicita antes do purge.',
    'purge.validation.jobActive': 'Xa hai un traballo de purge en execucion ({jobId}). Agarda a finalizacion.',
    'purge.validation.cancelled': 'Solicitude de purge cancelada antes do envio.',
    'purge.error.poll': 'Non se puido refrescar o estado do purge: {message}',
    'purge.error.jobNotFound': 'Non se atopou o traballo de purge {jobId}.',
    'purge.stageLabel': 'Fase',
    'purge.stage.queued': 'en cola',
    'purge.stage.dispatch': 'despacho',
    'purge.stage.purge_dataset': 'purge dataset',
    'purge.stage.full_rebuild': 'reconstrucion completa',
    'purge.stage.reload': 'recarga motor',
    'purge.stage.failed': 'fallido',
    'purge.summary.jobId': 'Job ID',
    'purge.summary.state': 'Estado',
    'purge.summary.message': 'Mensaxe',
    'purge.summary.targetDepartment': 'Departamento obxectivo',
    'purge.summary.dryRun': 'Dry run',
    'purge.summary.rowsBefore': 'Filas antes',
    'purge.summary.rowsRemoved': 'Filas eliminadas',
    'purge.summary.rowsAfter': 'Filas despois',
    'purge.summary.datasetPath': 'Saida dataset',
    'purge.summary.purgeSummaryPath': 'Resumo purge',
    'purge.summary.backupDatasetPath': 'Backup dataset',
    'purge.summary.rebuildStatus': 'Estado rebuild',
    'purge.summary.rebuildDuration': 'Duracion rebuild (s)',
    'purge.summary.rebuildIndexPath': 'Ruta indice rebuild',
    'purge.summary.rebuildVectorCount': 'Conta vectores rebuild',
    'purge.summary.rebuildDocCount': 'Conta docs rebuild',
    'purge.summary.rebuildSummaryPath': 'Resumo rebuild',
    'purge.summary.reloadGeneration': 'Xeracion recarga',
    'purge.summary.reloadLoadedAt': 'Recargado en',
    'purge.summary.error': 'Erro',
    'purge.state.queued': 'en cola',
    'purge.state.running': 'en execucion',
    'purge.state.succeeded': 'completado',
    'purge.state.failed': 'fallido',
    'purge.confirm.dialog': 'Confirmar purge para "{department}"? Executara purge, rebuild completo e recarga.',
    'purge.confirm.dialogDryRun': 'Confirmar dry-run para "{department}"? Non se escribiran cambios no dataset.',
    'panel.searchResults': 'Espazo de busca',
    'panel.inventory': 'Inventario de coñecemento',
    'catalog.title': 'Catalogo RAG',
    'catalog.subtitle': 'Explora webs, PDFs e fontes de tickets xa representadas no dataset.',
    'catalog.summary.total': '{total} fontes',
    'catalog.filters.kicker': 'Ferramentas do catalogo',
    'catalog.filters.toggle': 'Busca e filtros',
    'catalog.filters.toggleAriaExpand': 'Abrir busca e filtros',
    'catalog.filters.toggleAriaCollapse': 'Pechar busca e filtros',
    'catalog.form.searchLabel': 'Buscar no catalogo',
    'catalog.form.searchPlaceholder': 'Titulo, host, URL de orixe ou job de inxesta',
    'catalog.form.sourceTypeLabel': 'Tipo de fonte',
    'catalog.form.departmentLabel': 'Departamento',
    'catalog.form.departmentPlaceholder': 'Todos os departamentos ou un equipo',
    'catalog.form.submit': 'Aplicar filtros',
    'catalog.form.reset': 'Restablecer',
    'catalog.view.overview': 'Lista de fontes',
    'catalog.view.browse': 'Tarxetas',
    'catalog.type.all': 'Todas as fontes',
    'catalog.type.ticket': 'Tickets',
    'catalog.type.web': 'Webs',
    'catalog.type.pdf': 'PDFs',
    'catalog.status.loading': 'Cargando catalogo...',
    'catalog.status.loaded': '{total} fontes dispoñibles.',
    'catalog.status.error': 'Erro de catalogo: {message}',
    'catalog.empty': 'Non hai entradas do catalogo para os filtros actuais.',
    'catalog.meta.source': 'fonte',
    'catalog.meta.host': 'host',
    'catalog.meta.chunks': 'chunks',
    'catalog.meta.lastUpdated': 'ultima actualizacion',
    'catalog.meta.ingestedAt': 'inxestado',
    'catalog.meta.jobId': 'job',
    'catalog.overview.kicker': 'Mapa de fontes',
    'catalog.overview.subtitle': 'Abre un grupo e revisa as paxinas, PDFs ou tickets que contén agora mesmo.',
    'catalog.overview.loading': 'Cargando lista de fontes...',
    'catalog.overview.empty': 'Non hai grupos de fontes para os filtros actuais.',
    'catalog.overview.documents': '{count} elementos',
    'catalog.overview.departments': '{count} departamentos',
    'catalog.overview.groupCount': '{count} grupos',
    'catalog.overview.pages': '{count} paxinas',
    'catalog.overview.pdfs': '{count} PDFs',
    'catalog.overview.tickets': '{count} tickets',
    'catalog.overview.open': 'Abrir',
    'catalog.overview.noPath': 'Sen ruta',
    'catalog.link.open': 'abrir fonte',
    'catalog.validation.departmentInvalid': 'Filtro de departamento invalido. Usa 3-32 caracteres: letras, numeros, "_" ou "-".',
    'common.yes': 'si',
    'common.no': 'no',
  },
});

const FLATPICKR_LOCALES = Object.freeze({
  es: {
    weekdays: {
      shorthand: ['Do', 'Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa'],
      longhand: ['Domingo', 'Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado'],
    },
    months: {
      shorthand: ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
      longhand: ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'],
    },
    firstDayOfWeek: 1,
    time_24hr: true,
  },
  gl: {
    weekdays: {
      shorthand: ['Do', 'Lu', 'Ma', 'Me', 'Xo', 'Ve', 'Sa'],
      longhand: ['Domingo', 'Luns', 'Martes', 'Mercores', 'Xoves', 'Venres', 'Sabado'],
    },
    months: {
      shorthand: ['Xan', 'Feb', 'Mar', 'Abr', 'Mai', 'Xun', 'Xul', 'Ago', 'Set', 'Out', 'Nov', 'Dec'],
      longhand: ['Xaneiro', 'Febreiro', 'Marzo', 'Abril', 'Maio', 'Xuno', 'Xullo', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Decembro'],
    },
    firstDayOfWeek: 1,
    time_24hr: true,
  },
});

const DEPARTMENT_ALIASES = Object.freeze({
  all: 'all',
  aplicacion: 'aplicaciones',
  aplicaiones: 'aplicaciones',
  aplicacions: 'aplicaciones',
  aplicaciones: 'aplicaciones',
  application: 'aplicaciones',
  applications: 'aplicaciones',
  sistema: 'sistemas',
  systems: 'sistemas',
  system: 'sistemas',
  sistemas: 'sistemas',
  bigdata: 'bigdata',
  'big data': 'bigdata',
  'big-data': 'bigdata',
  big_data: 'bigdata',
  bd: 'bigdata',
  slurm: 'slurm',
});

let dateFromPicker = null;
let dateToPicker = null;
let isSubmitting = false;
const monthSelectState = new WeakMap();
let activeWorkspaceTab = 'search';
let activeCatalogView = 'overview';
let catalogFiltersExpanded = false;
let currentResults = [];
let currentPage = 1;
let hasRenderedResults = false;
let currentLanguage = 'en';
let currentStatusState = {
  kind: 'key',
  key: 'status.ready',
  params: {},
  error: false,
  showSpinner: false,
};
let catalogIsLoading = true;
let catalogHasRendered = false;
let catalogErrorMessage = '';
let catalogModel = {
  total: 0,
  page: 1,
  page_size: CATALOG_PAGE_SIZE,
  has_more: false,
  items: [],
};
let catalogStatusState = {
  kind: 'key',
  key: 'catalog.status.loading',
  params: {},
  error: false,
  showSpinner: true,
  message: '',
};
let catalogOverviewIsLoading = true;
let catalogOverviewHasRendered = false;
let catalogOverviewErrorMessage = '';
let catalogOverviewModel = {
  total_groups: 0,
  total_items: 0,
  groups: [],
};
let ingestIsSubmitting = false;
let ingestActiveJobId = null;
let ingestPollTimer = null;
let ingestStatusModel = null;
let ingestStatusState = {
  kind: 'key',
  key: 'ingest.status.idle',
  params: {},
  error: false,
  showSpinner: false,
  message: '',
};
let purgeIsSubmitting = false;
let purgeActiveJobId = null;
let purgePollTimer = null;
let purgeStatusModel = null;
let purgeStatusState = {
  kind: 'key',
  key: 'purge.status.idle',
  params: {},
  error: false,
  showSpinner: false,
  message: '',
};
let availableSearchDepartments = [...DEFAULT_SEARCH_DEPARTMENTS];

function canonicalDepartment(value) {
  const raw = (value || '').toLowerCase().trim();
  return DEPARTMENT_ALIASES[raw] || null;
}

function normalizeDepartmentValue(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return 'all';
  const canonical = canonicalDepartment(raw);
  if (canonical) return canonical;
  return availableSearchDepartments.includes(raw) ? raw : 'all';
}

function normalizeLanguage(value) {
  const raw = String(value || '').toLowerCase().trim();
  if (raw.startsWith('es')) return 'es';
  if (raw.startsWith('gl')) return 'gl';
  if (raw.startsWith('en')) return 'en';
  return 'en';
}

function detectInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANGUAGE_STORAGE_KEY);
    if (stored) return normalizeLanguage(stored);
  } catch {
    // no-op: localStorage might be blocked.
  }

  if (Array.isArray(navigator.languages) && navigator.languages.length > 0) {
    return normalizeLanguage(navigator.languages[0]);
  }
  return normalizeLanguage(navigator.language || 'en');
}

function setLanguage(language, { persist = true } = {}) {
  currentLanguage = normalizeLanguage(language);
  if (persist) {
    try {
      window.localStorage.setItem(LANGUAGE_STORAGE_KEY, currentLanguage);
    } catch {
      // no-op: localStorage might be blocked.
    }
  }
  applyTranslations();
}

function t(key, params = {}) {
  const active = I18N[currentLanguage] || I18N.en;
  const fallback = I18N.en;
  let template = active[key];
  if (typeof template !== 'string') template = fallback[key];
  if (typeof template !== 'string') template = key;

  return template.replace(/\{([a-zA-Z0-9_]+)\}/g, (_, token) => {
    if (Object.prototype.hasOwnProperty.call(params, token)) {
      return String(params[token]);
    }
    return `{${token}}`;
  });
}

function escapeHTML(str) {
  if (str == null) return '';
  const d = document.createElement('div');
  d.textContent = String(str);
  return d.innerHTML;
}

function safeLinkURL(url) {
  if (!url) return '';
  try {
    const parsed = new URL(url);
    return (parsed.protocol === 'http:' || parsed.protocol === 'https:') ? parsed.href : '';
  } catch {
    return '';
  }
}

function isCanonicalDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function toApiDate(value) {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;

  if (isCanonicalDate(raw)) return raw;

  const dmy = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (dmy) {
    return `${dmy[3]}-${dmy[2]}-${dmy[1]}`;
  }

  const isoWithTime = raw.match(/^(\d{4}-\d{2}-\d{2})[ T].+$/);
  if (isoWithTime) {
    return isoWithTime[1];
  }

  return raw;
}

function formatApiError(err, status) {
  if (!err || typeof err !== 'object') {
    return t('error.httpStatus', { status });
  }

  const detail = err.detail;
  if (typeof detail === 'string' && detail.trim()) {
    return detail.trim();
  }

  if (Array.isArray(detail) && detail.length > 0) {
    const lines = detail.map((item) => {
      if (!item || typeof item !== 'object') {
        return String(item);
      }
      const loc = Array.isArray(item.loc)
        ? item.loc.filter((part) => part !== 'body').join('.')
        : '';
      const msg = item.msg ? String(item.msg) : JSON.stringify(item);
      return loc ? `${loc}: ${msg}` : msg;
    });
    return lines.join(' | ');
  }

  if (detail && typeof detail === 'object') {
    if (typeof detail.message === 'string' && detail.message.trim()) {
      const message = detail.message.trim();
      const errors = Array.isArray(detail.extra?.errors) ? detail.extra.errors : [];
      if (!errors.length) {
        return message;
      }
      const parts = errors.slice(0, 3).map((item) => {
        if (!item || typeof item !== 'object') return String(item);
        const loc = Array.isArray(item.loc)
          ? item.loc.filter((part) => part !== 'body').join('.')
          : '';
        const msg = item.msg ? String(item.msg) : JSON.stringify(item);
        return loc ? `${loc}: ${msg}` : msg;
      });
      return `${message} (${parts.join(' | ')})`;
    }
    try {
      return JSON.stringify(detail);
    } catch {
      // no-op
    }
  }

  if (typeof err.message === 'string' && err.message.trim()) {
    return err.message.trim();
  }

  return t('error.httpStatus', { status });
}

function teardownMonthSelect(instance) {
  const state = monthSelectState.get(instance);
  if (!state) return;
  document.removeEventListener('mousedown', state.onDocumentMouseDown);
  document.removeEventListener('keydown', state.onDocumentKeydown);
  if (state.wrapper?.isConnected) {
    state.wrapper.remove();
  }
  monthSelectState.delete(instance);
}

function setMonthMenuOpen(instance, isOpen) {
  const state = monthSelectState.get(instance);
  if (!state) return;
  state.wrapper.classList.toggle('open', Boolean(isOpen));
  state.trigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
}

function closeMonthMenu(instance) {
  setMonthMenuOpen(instance, false);
}

function ensureCustomMonthDropdown(instance) {
  const calendar = instance?.calendarContainer;
  const currentMonthWrap = calendar?.querySelector('.flatpickr-current-month');
  const nativeMonthSelect = calendar?.querySelector('.flatpickr-monthDropdown-months');
  if (!calendar || !currentMonthWrap || !nativeMonthSelect) return;

  const monthNames = (instance.l10n?.months?.longhand || []).slice(0, 12);
  if (monthNames.length !== 12) return;

  const currentState = monthSelectState.get(instance);
  if (currentState?.wrapper && !currentState.wrapper.isConnected) {
    teardownMonthSelect(instance);
  }

  if (monthSelectState.has(instance)) {
    if (!currentMonthWrap.contains(monthSelectState.get(instance).wrapper)) {
      currentMonthWrap.insertBefore(monthSelectState.get(instance).wrapper, currentMonthWrap.firstChild);
    }
    nativeMonthSelect.classList.add('month-native-hidden');
    return;
  }

  nativeMonthSelect.classList.add('month-native-hidden');

  const wrapper = document.createElement('div');
  wrapper.className = 'custom-select month-custom-select';

  const trigger = document.createElement('button');
  trigger.type = 'button';
  trigger.className = 'custom-select-trigger month-custom-trigger';
  trigger.setAttribute('aria-haspopup', 'listbox');
  trigger.setAttribute('aria-expanded', 'false');

  const menu = document.createElement('ul');
  menu.className = 'custom-select-menu month-custom-menu';
  menu.setAttribute('role', 'listbox');
  menu.tabIndex = -1;

  for (let idx = 0; idx < monthNames.length; idx += 1) {
    const item = document.createElement('li');
    item.className = 'custom-select-option month-custom-option';
    item.dataset.month = String(idx);
    item.setAttribute('role', 'option');
    item.textContent = monthNames[idx];
    menu.appendChild(item);
  }

  wrapper.appendChild(trigger);
  wrapper.appendChild(menu);
  currentMonthWrap.insertBefore(wrapper, currentMonthWrap.firstChild);

  trigger.addEventListener('click', (ev) => {
    ev.stopPropagation();
    const isOpen = !wrapper.classList.contains('open');
    setMonthMenuOpen(instance, isOpen);
  });

  menu.addEventListener('click', (ev) => {
    const option = ev.target.closest('.custom-select-option');
    if (!option) return;
    const targetMonth = Number(option.dataset.month);
    if (!Number.isNaN(targetMonth)) {
      instance.changeMonth(targetMonth, false);
      syncCustomMonthDropdown(instance);
    }
    closeMonthMenu(instance);
  });

  const onDocumentMouseDown = (ev) => {
    if (wrapper.isConnected && !wrapper.contains(ev.target)) closeMonthMenu(instance);
  };
  const onDocumentKeydown = (ev) => {
    if (ev.key === 'Escape') closeMonthMenu(instance);
  };
  document.addEventListener('mousedown', onDocumentMouseDown);
  document.addEventListener('keydown', onDocumentKeydown);

  monthSelectState.set(instance, {
    wrapper,
    trigger,
    menu,
    onDocumentMouseDown,
    onDocumentKeydown,
  });
}

function syncCustomMonthDropdown(instance) {
  ensureCustomMonthDropdown(instance);
  const state = monthSelectState.get(instance);
  if (!state) return;

  const monthNames = (instance.l10n?.months?.longhand || []).slice(0, 12);
  if (monthNames.length === 12) {
    for (let idx = 0; idx < monthNames.length; idx += 1) {
      const option = state.menu.querySelector(`.custom-select-option[data-month="${idx}"]`);
      if (option) option.textContent = monthNames[idx];
    }
  }

  const month = String(instance.currentMonth);
  const selected = state.menu.querySelector(`.custom-select-option[data-month="${month}"]`);
  if (selected) state.trigger.textContent = selected.textContent || '';
  for (const item of state.menu.querySelectorAll('.custom-select-option')) {
    const isSelected = item.dataset.month === month;
    item.classList.toggle('selected', isSelected);
    item.setAttribute('aria-selected', isSelected ? 'true' : 'false');
  }
}

function departmentOptionLabel(value) {
  const canonical = canonicalDepartment(value);
  if (canonical === 'all') return t('department.option.all');
  if (canonical === 'aplicaciones') return t('department.option.aplicaciones');
  if (canonical === 'sistemas') return t('department.option.sistemas');
  if (canonical === 'bigdata') return t('department.option.bigdata');
  if (canonical === 'slurm') return t('department.option.slurm');
  return String(value || '').trim();
}

function normalizeSearchDepartmentOptions(values) {
  const normalized = [...DEFAULT_SEARCH_DEPARTMENTS];
  const seen = new Set(normalized);

  if (Array.isArray(values)) {
    for (const value of values) {
      const raw = String(value || '').trim().toLowerCase();
      if (!raw) continue;
      const canonical = canonicalDepartment(raw);
      const checked = validIngestDepartment(raw);
      const department = canonical || checked.department;
      if (!department || seen.has(department)) continue;
      seen.add(department);
      normalized.push(department);
    }
  }

  return normalized;
}

function renderDepartmentOptions(values) {
  if (!departmentMenuEl) return;
  availableSearchDepartments = normalizeSearchDepartmentOptions(values);
  const selectedBeforeRender = departmentEl?.value || 'all';
  departmentMenuEl.innerHTML = '';

  for (const value of availableSearchDepartments) {
    const option = document.createElement('li');
    option.className = 'custom-select-option';
    option.setAttribute('role', 'option');
    option.setAttribute('aria-selected', 'false');
    option.dataset.value = value;
    option.textContent = departmentOptionLabel(value);
    departmentMenuEl.appendChild(option);
  }

  const adminDepartmentOptions = availableSearchDepartments.filter((value) => value !== 'all');
  for (const list of [ingestDepartmentOptionsEl, purgeDepartmentOptionsEl, catalogDepartmentOptionsEl]) {
    if (!list) continue;
    list.innerHTML = '';
    for (const value of adminDepartmentOptions) {
      const option = document.createElement('option');
      option.value = value;
      list.appendChild(option);
    }
  }

  setDepartmentValue(selectedBeforeRender);
}

async function loadSearchDepartments() {
  try {
    const response = await fetch(apiUrl('/search/departments'), { method: 'GET' });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json().catch(() => ({}));
    renderDepartmentOptions(Array.isArray(payload.departments) ? payload.departments : []);
  } catch {
    renderDepartmentOptions(availableSearchDepartments);
  }
}

function setDepartmentValue(value) {
  let normalized = normalizeDepartmentValue(value);
  let option = departmentMenuEl?.querySelector(`.custom-select-option[data-value="${normalized}"]`);
  if (!option) {
    normalized = 'all';
    option = departmentMenuEl?.querySelector('.custom-select-option[data-value="all"]');
  }
  if (!departmentEl || !departmentTriggerEl || !departmentMenuEl || !option) return;

  departmentEl.value = normalized;
  departmentTriggerEl.textContent = option.textContent.trim();
  for (const node of departmentMenuEl.querySelectorAll('.custom-select-option')) {
    const selected = node === option;
    node.classList.toggle('selected', selected);
    node.setAttribute('aria-selected', selected ? 'true' : 'false');
  }
}

function setLanguageValue(value) {
  const normalized = normalizeLanguage(value);
  const option = languageMenuEl?.querySelector(`.custom-select-option[data-value="${normalized}"]`);
  if (!languageSelectorEl || !languageTriggerEl || !languageMenuEl || !option) return;

  languageSelectorEl.value = normalized;
  languageTriggerEl.textContent = option.textContent.trim();
  for (const node of languageMenuEl.querySelectorAll('.custom-select-option')) {
    const selected = node === option;
    node.classList.toggle('selected', selected);
    node.setAttribute('aria-selected', selected ? 'true' : 'false');
  }
}

function setCatalogSourceTypeValue(value) {
  const normalized = normalizeCatalogSourceType(value);
  let option = catalogSourceTypeMenuEl?.querySelector(`.custom-select-option[data-value="${normalized}"]`);
  if (!option) {
    option = catalogSourceTypeMenuEl?.querySelector('.custom-select-option[data-value=""]');
  }
  if (!catalogSourceTypeEl || !catalogSourceTypeTriggerEl || !catalogSourceTypeMenuEl || !option) return;

  catalogSourceTypeEl.value = normalized;
  catalogSourceTypeTriggerEl.textContent = option.textContent.trim();
  for (const node of catalogSourceTypeMenuEl.querySelectorAll('.custom-select-option')) {
    const selected = node === option;
    node.classList.toggle('selected', selected);
    node.setAttribute('aria-selected', selected ? 'true' : 'false');
  }
  renderCatalogFiltersDrawer();
}

function closeDepartmentMenu() {
  if (!departmentSelectEl || !departmentTriggerEl) return;
  departmentSelectEl.classList.remove('open');
  departmentTriggerEl.setAttribute('aria-expanded', 'false');
}

function closeCatalogSourceTypeMenu() {
  if (!catalogSourceTypeSelectEl || !catalogSourceTypeTriggerEl) return;
  catalogSourceTypeSelectEl.classList.remove('open');
  catalogSourceTypeTriggerEl.setAttribute('aria-expanded', 'false');
}

function closeLanguageMenu() {
  if (!languageSelectEl || !languageTriggerEl) return;
  languageSelectEl.classList.remove('open');
  languageTriggerEl.setAttribute('aria-expanded', 'false');
}

function initDepartmentSelect() {
  if (!departmentSelectEl || !departmentTriggerEl || !departmentMenuEl || !departmentEl) return;

  departmentTriggerEl.addEventListener('click', () => {
    const isOpen = departmentSelectEl.classList.toggle('open');
    departmentTriggerEl.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  });

  departmentMenuEl.addEventListener('click', (ev) => {
    const option = ev.target.closest('.custom-select-option');
    if (!option) return;
    setDepartmentValue(option.dataset.value || 'all');
    closeDepartmentMenu();
  });

  document.addEventListener('click', (ev) => {
    if (!departmentSelectEl.contains(ev.target)) {
      closeDepartmentMenu();
    }
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape') closeDepartmentMenu();
  });

  renderDepartmentOptions(availableSearchDepartments);
}

function initCatalogSourceTypeSelect() {
  if (!catalogSourceTypeSelectEl || !catalogSourceTypeTriggerEl || !catalogSourceTypeMenuEl || !catalogSourceTypeEl) return;

  catalogSourceTypeTriggerEl.addEventListener('click', () => {
    const isOpen = catalogSourceTypeSelectEl.classList.toggle('open');
    catalogSourceTypeTriggerEl.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  });

  catalogSourceTypeMenuEl.addEventListener('click', (ev) => {
    const option = ev.target.closest('.custom-select-option');
    if (!option) return;
    setCatalogSourceTypeValue(option.dataset.value || '');
    closeCatalogSourceTypeMenu();
  });

  document.addEventListener('click', (ev) => {
    if (!catalogSourceTypeSelectEl.contains(ev.target)) {
      closeCatalogSourceTypeMenu();
    }
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape') closeCatalogSourceTypeMenu();
  });

  setCatalogSourceTypeValue(catalogSourceTypeEl.value || '');
}

function initLanguageSelect() {
  if (!languageSelectEl || !languageTriggerEl || !languageMenuEl || !languageSelectorEl) return;

  languageTriggerEl.addEventListener('click', () => {
    const isOpen = languageSelectEl.classList.toggle('open');
    languageTriggerEl.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  });

  languageMenuEl.addEventListener('click', (ev) => {
    const option = ev.target.closest('.custom-select-option');
    if (!option) return;
    setLanguage(option.dataset.value || 'en');
    closeLanguageMenu();
  });

  document.addEventListener('click', (ev) => {
    if (!languageSelectEl.contains(ev.target)) {
      closeLanguageMenu();
    }
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape') closeLanguageMenu();
  });

  setLanguageValue(currentLanguage);
}

function setLoadingState(isLoading) {
  document.body.classList.toggle('is-loading', Boolean(isLoading));
}

function renderStatus() {
  if (!statusEl) return;

  const message = currentStatusState.kind === 'key'
    ? t(currentStatusState.key, currentStatusState.params || {})
    : String(currentStatusState.message || '');

  if (currentStatusState.showSpinner) {
    statusEl.innerHTML = '<span class="spinner"></span>' + escapeHTML(message);
  } else {
    statusEl.textContent = message;
  }

  statusEl.className = currentStatusState.error ? 'status error' : 'status';
  if (!currentStatusState.showSpinner) {
    setLoadingState(false);
  }
}

function setStatusKey(key, options = {}) {
  currentStatusState = {
    kind: 'key',
    key,
    params: options.params || {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
  };
  renderStatus();
}

function renderWorkspaceTabs() {
  const isSearch = activeWorkspaceTab === 'search';
  if (workspaceTabSearchEl) {
    workspaceTabSearchEl.classList.toggle('is-active', isSearch);
    workspaceTabSearchEl.setAttribute('aria-selected', isSearch ? 'true' : 'false');
    workspaceTabSearchEl.tabIndex = isSearch ? 0 : -1;
  }
  if (workspaceTabCatalogEl) {
    workspaceTabCatalogEl.classList.toggle('is-active', !isSearch);
    workspaceTabCatalogEl.setAttribute('aria-selected', isSearch ? 'false' : 'true');
    workspaceTabCatalogEl.tabIndex = isSearch ? -1 : 0;
  }
  if (workspacePanelSearchEl) workspacePanelSearchEl.hidden = !isSearch;
  if (workspacePanelCatalogEl) workspacePanelCatalogEl.hidden = isSearch;
}

function setWorkspaceTab(tab, options = {}) {
  activeWorkspaceTab = tab === 'catalog' ? 'catalog' : 'search';
  renderWorkspaceTabs();
  if (options.focusTrigger) {
    if (activeWorkspaceTab === 'catalog') {
      workspaceTabCatalogEl?.focus();
    } else {
      workspaceTabSearchEl?.focus();
    }
  }
}

function renderCatalogViewTabs() {
  const isOverview = activeCatalogView === 'overview';
  if (catalogViewTabOverviewEl) {
    catalogViewTabOverviewEl.classList.toggle('is-active', isOverview);
    catalogViewTabOverviewEl.setAttribute('aria-selected', isOverview ? 'true' : 'false');
    catalogViewTabOverviewEl.tabIndex = isOverview ? 0 : -1;
  }
  if (catalogViewTabBrowseEl) {
    catalogViewTabBrowseEl.classList.toggle('is-active', !isOverview);
    catalogViewTabBrowseEl.setAttribute('aria-selected', isOverview ? 'false' : 'true');
    catalogViewTabBrowseEl.tabIndex = isOverview ? -1 : 0;
  }
  if (catalogViewPanelOverviewEl) catalogViewPanelOverviewEl.hidden = !isOverview;
  if (catalogViewPanelBrowseEl) catalogViewPanelBrowseEl.hidden = isOverview;
}

function setCatalogView(view, options = {}) {
  activeCatalogView = view === 'browse' ? 'browse' : 'overview';
  renderCatalogViewTabs();
  if (options.focusTrigger) {
    if (activeCatalogView === 'browse') {
      catalogViewTabBrowseEl?.focus();
    } else {
      catalogViewTabOverviewEl?.focus();
    }
  }
}

function catalogHasActiveFilters() {
  return Boolean(
    String(catalogQueryEl?.value || '').trim()
    || normalizeCatalogSourceType(catalogSourceTypeEl?.value || '')
    || normalizeCatalogDepartmentFilter(catalogDepartmentEl?.value || '').department
  );
}

function renderCatalogFiltersDrawer() {
  const shell = catalogFiltersToggleEl?.closest('.catalog-filters-shell');
  const hasActiveFilters = catalogHasActiveFilters();
  if (catalogFiltersToggleEl) {
    if (catalogFiltersPanelEl?.id) {
      catalogFiltersToggleEl.setAttribute('aria-controls', catalogFiltersPanelEl.id);
    }
    catalogFiltersToggleEl.setAttribute(
      'aria-label',
      catalogFiltersExpanded ? t('catalog.filters.toggleAriaCollapse') : t('catalog.filters.toggleAriaExpand'),
    );
    catalogFiltersToggleEl.setAttribute('aria-expanded', catalogFiltersExpanded ? 'true' : 'false');
    catalogFiltersToggleEl.classList.toggle('has-active-filters', hasActiveFilters);
    catalogFiltersToggleEl.classList.toggle('is-open', catalogFiltersExpanded);
    catalogFiltersToggleEl.classList.toggle('is-collapsed', !catalogFiltersExpanded);
  }
  if (catalogFiltersPanelEl) {
    catalogFiltersPanelEl.hidden = !catalogFiltersExpanded;
    catalogFiltersPanelEl.classList.toggle('is-open', catalogFiltersExpanded);
    catalogFiltersPanelEl.classList.toggle('is-collapsed', !catalogFiltersExpanded);
  }
  if (shell) {
    shell.classList.toggle('has-active-filters', hasActiveFilters);
    shell.classList.toggle('is-open', catalogFiltersExpanded);
    shell.classList.toggle('is-collapsed', !catalogFiltersExpanded);
  }
}

function setCatalogFiltersExpanded(expanded, options = {}) {
  catalogFiltersExpanded = Boolean(expanded);
  renderCatalogFiltersDrawer();
  if (catalogFiltersExpanded && options.focusPanel) {
    catalogQueryEl?.focus();
  }
  if (!catalogFiltersExpanded && options.focusTrigger) {
    catalogFiltersToggleEl?.focus();
  }
}

function renderCatalogStatus() {
  if (!catalogStatusEl) return;

  const message = catalogStatusState.kind === 'key'
    ? t(catalogStatusState.key, catalogStatusState.params || {})
    : String(catalogStatusState.message || '');

  if (catalogStatusState.showSpinner) {
    catalogStatusEl.innerHTML = '<span class="spinner"></span>' + escapeHTML(message);
  } else {
    catalogStatusEl.textContent = message;
  }

  catalogStatusEl.className = catalogStatusState.error ? 'status error' : 'status';
}

function catalogOverviewEmptyMarkup(message) {
  return `
    <div class="empty-state">
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#8ea0b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
        <rect x="3" y="4" width="18" height="16" rx="2"/>
        <line x1="7" y1="9" x2="17" y2="9"/>
        <line x1="7" y1="13" x2="17" y2="13"/>
      </svg>
      <p>${escapeHTML(message)}</p>
    </div>
  `;
}

function setCatalogStatusKey(key, options = {}) {
  catalogStatusState = {
    kind: 'key',
    key,
    params: options.params || {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
    message: '',
  };
  renderCatalogStatus();
}

function setCatalogStatusMessage(message, options = {}) {
  catalogStatusState = {
    kind: 'message',
    key: '',
    params: {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
    message: String(message || ''),
  };
  renderCatalogStatus();
}

function normalizeCatalogSourceType(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw || raw === 'all') return '';
  if (raw === 'html' || raw === 'website') return 'web';
  return ['ticket', 'web', 'pdf'].includes(raw) ? raw : '';
}

function normalizeCatalogDepartmentFilter(value) {
  const raw = String(value || '').trim();
  if (!raw || raw.toLowerCase() === 'all') {
    return { department: '', reason: null };
  }

  const checked = validIngestDepartment(raw);
  if (!checked.department) {
    return { department: null, reason: checked.reason || 'invalid' };
  }
  return { department: checked.department, reason: null };
}

function formatCatalogSourceType(value) {
  const normalized = normalizeCatalogSourceType(value) || 'ticket';
  const key = `catalog.type.${normalized}`;
  const translated = t(key);
  return translated === key ? normalized : translated;
}

function updateCatalogTotalChip(total) {
  if (!catalogTotalChipEl) return;
  catalogTotalChipEl.textContent = t('catalog.summary.total', {
    total: Number.isFinite(Number(total)) ? Number(total) : 0,
  });
}

function catalogActiveFilters() {
  const departmentState = normalizeCatalogDepartmentFilter(catalogDepartmentEl?.value || '');
  return {
    q: String(catalogQueryEl?.value || '').trim(),
    sourceType: normalizeCatalogSourceType(catalogSourceTypeEl?.value || ''),
    departmentState,
  };
}

function catalogEmptyMarkup(message) {
  return `
    <div class="empty-state">
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#8ea0b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
        <line x1="8" y1="11" x2="14" y2="11"/>
      </svg>
      <p>${escapeHTML(message)}</p>
    </div>
  `;
}

function catalogTreeGroupMeta(group) {
  const parts = [
    t('catalog.overview.documents', { count: Number(group?.total_children || 0) }),
  ];
  if (Number(group?.web_count || 0) > 0) {
    parts.push(t('catalog.overview.pages', { count: Number(group.web_count) }));
  }
  if (Number(group?.pdf_count || 0) > 0) {
    parts.push(t('catalog.overview.pdfs', { count: Number(group.pdf_count) }));
  }
  if (Number(group?.ticket_count || 0) > 0) {
    parts.push(t('catalog.overview.tickets', { count: Number(group.ticket_count) }));
  }
  parts.push(`${t('catalog.meta.chunks')}: ${Number(group?.total_chunks || 0)}`);
  return parts;
}

function catalogTreeChildMeta(child) {
  const parts = [
    formatCatalogSourceType(child?.source_type),
    `${t('card.department')}: ${formatDepartmentDisplay(child?.department)}`,
    `${t('catalog.meta.chunks')}: ${Number(child?.chunk_count || 0)}`,
  ];
  if (child?.last_updated) {
    parts.push(`${t('catalog.meta.lastUpdated')}: ${child.last_updated}`);
  } else if (child?.ingested_at) {
    parts.push(`${t('catalog.meta.ingestedAt')}: ${child.ingested_at}`);
  }
  return parts;
}

function renderCatalogOverview() {
  if (!catalogOverviewResultsEl) return;

  if (catalogOverviewIsLoading && !catalogOverviewHasRendered) {
    catalogOverviewResultsEl.innerHTML = catalogOverviewEmptyMarkup(t('catalog.overview.loading'));
    return;
  }

  if (catalogOverviewErrorMessage) {
    catalogOverviewResultsEl.innerHTML = catalogOverviewEmptyMarkup(catalogOverviewErrorMessage);
    return;
  }

  const groups = Array.isArray(catalogOverviewModel.groups) ? catalogOverviewModel.groups : [];
  if (!groups.length) {
    catalogOverviewResultsEl.innerHTML = catalogOverviewEmptyMarkup(t('catalog.overview.empty'));
    return;
  }

  catalogOverviewResultsEl.innerHTML = '';
  for (const group of groups) {
    const details = document.createElement('details');
    details.className = 'catalog-tree-group';
    const groupMeta = catalogTreeGroupMeta(group)
      .map((part) => `<span>${escapeHTML(part)}</span>`)
      .join('');
    const description = String(group?.description || group?.host || '').trim();
    details.innerHTML = `
      <summary class="catalog-tree-summary">
        <div class="catalog-tree-summary-copy">
          <div class="catalog-tree-summary-title-row">
            <h3>${escapeHTML(String(group?.label || 'Sources'))}</h3>
            <span class="catalog-tree-summary-pill">${escapeHTML(t('catalog.overview.documents', { count: Number(group?.total_children || 0) }))}</span>
          </div>
          ${description ? `<p>${escapeHTML(description)}</p>` : ''}
          <div class="catalog-tree-summary-meta">${groupMeta}</div>
        </div>
      </summary>
      <div class="catalog-tree-children"></div>
    `;

    const childrenEl = details.querySelector('.catalog-tree-children');
    const children = Array.isArray(group?.children) ? group.children : [];
    if (childrenEl) {
      for (const child of children) {
        const safeSource = safeLinkURL(child?.source);
        const childPath = String(child?.path || '').trim() || t('catalog.overview.noPath');
        const childMeta = catalogTreeChildMeta(child)
          .map((part) => `<span>${escapeHTML(part)}</span>`)
          .join('');
        const article = document.createElement('article');
        const sourceType = normalizeCatalogSourceType(child?.source_type) || 'ticket';
        article.className = `catalog-tree-item catalog-tree-item--${sourceType}`;
        article.innerHTML = `
          <div class="catalog-tree-item-head">
            <div class="catalog-tree-item-title-wrap">
              <span class="catalog-type-badge catalog-type-badge--${sourceType}">${escapeHTML(formatCatalogSourceType(sourceType))}</span>
              <h4>${escapeHTML(String(child?.title || '-'))}</h4>
            </div>
            ${safeSource ? `<a class="catalog-tree-open" href="${escapeHTML(safeSource)}" target="_blank" rel="noopener">${escapeHTML(t('catalog.overview.open'))}</a>` : ''}
          </div>
          <div class="catalog-tree-path">${escapeHTML(childPath)}</div>
          <div class="catalog-tree-item-meta">${childMeta}</div>
        `;
        childrenEl.appendChild(article);
      }
    }
    catalogOverviewResultsEl.appendChild(details);
  }
}

function renderCatalogPagination() {
  if (!catalogPaginationEl) return;

  const total = Number(catalogModel.total || 0);
  const pageSize = Number(catalogModel.page_size || CATALOG_PAGE_SIZE);
  const page = Number(catalogModel.page || 1);
  const totalPages = Math.ceil(total / pageSize);
  if (totalPages <= 1) {
    catalogPaginationEl.innerHTML = '';
    return;
  }

  const start = (page - 1) * pageSize + 1;
  const end = Math.min(page * pageSize, total);
  const items = paginationItems(totalPages, page);
  const controls = items.map((item) => {
    if (item === '...') {
      return '<span class="pagination-ellipsis">...</span>';
    }
    const activeClass = item === page ? ' is-active' : '';
    return `<button class="page-btn${activeClass}" data-page="${item}" type="button">${item}</button>`;
  }).join('');

  catalogPaginationEl.innerHTML = `
    <div class="pagination-summary">${escapeHTML(t('results.summary', { start, end, total }))}</div>
    <div class="pagination-controls">
      <button class="page-btn" type="button" data-page="${page - 1}" ${page <= 1 ? 'disabled' : ''}>${escapeHTML(t('pagination.prev'))}</button>
      ${controls}
      <button class="page-btn" type="button" data-page="${page + 1}" ${page >= totalPages ? 'disabled' : ''}>${escapeHTML(t('pagination.next'))}</button>
    </div>
  `;
}

function renderCatalogResults() {
  if (!catalogResultsEl) return;

  updateCatalogTotalChip(catalogModel.total);
  catalogResultsEl.innerHTML = '';

  if (catalogIsLoading && !catalogHasRendered) {
    catalogResultsEl.innerHTML = catalogEmptyMarkup(t('catalog.status.loading'));
    if (catalogPaginationEl) catalogPaginationEl.innerHTML = '';
    return;
  }

  if (catalogErrorMessage) {
    catalogResultsEl.innerHTML = catalogEmptyMarkup(catalogErrorMessage);
    if (catalogPaginationEl) catalogPaginationEl.innerHTML = '';
    return;
  }

  const items = Array.isArray(catalogModel.items) ? catalogModel.items : [];
  if (!items.length) {
    catalogResultsEl.innerHTML = catalogEmptyMarkup(t('catalog.empty'));
    if (catalogPaginationEl) catalogPaginationEl.innerHTML = '';
    return;
  }

  for (const [index, item] of items.entries()) {
    const card = document.createElement('article');
    const sourceType = normalizeCatalogSourceType(item?.source_type) || 'ticket';
    const sourceLabel = formatCatalogSourceType(sourceType);
    const departmentText = formatDepartmentDisplay(item?.department);
    const safeSource = safeLinkURL(item?.source);
    const title = String(item?.title || item?.source || '-').trim() || '-';
    const source = String(item?.source || '').trim();
    const host = String(item?.host || '').trim();
    const chunkCount = Number(item?.chunk_count || 0);
    const lastUpdated = String(item?.last_updated || '').trim();
    const ingestedAt = String(item?.ingested_at || '').trim();
    const ingestJobId = String(item?.ingest_job_id || '').trim();

    card.className = `catalog-card catalog-card--${sourceType}`;
    card.style.setProperty('--card-index', String(index));
    card.innerHTML = `
      <div class="catalog-topline">
        <span class="catalog-type-badge catalog-type-badge--${sourceType}">${escapeHTML(sourceLabel)}</span>
        <span class="dept-badge">${escapeHTML(t('card.department'))}: ${escapeHTML(departmentText)}</span>
      </div>
      <h3 class="catalog-card-title">${escapeHTML(title)}</h3>
      ${source ? `<div class="catalog-source-row">${safeSource
        ? `<a class="catalog-source-link" href="${escapeHTML(safeSource)}" target="_blank" rel="noopener">${escapeHTML(t('catalog.link.open'))}</a>`
        : `<span class="catalog-source-fallback">${escapeHTML(source)}</span>`}</div>` : ''}
      <div class="catalog-meta-grid">
        ${source ? `<span><strong>${escapeHTML(t('catalog.meta.source'))}:</strong> ${escapeHTML(source)}</span>` : ''}
        ${host ? `<span><strong>${escapeHTML(t('catalog.meta.host'))}:</strong> ${escapeHTML(host)}</span>` : ''}
        <span><strong>${escapeHTML(t('catalog.meta.chunks'))}:</strong> ${escapeHTML(chunkCount)}</span>
        ${lastUpdated ? `<span><strong>${escapeHTML(t('catalog.meta.lastUpdated'))}:</strong> ${escapeHTML(lastUpdated)}</span>` : ''}
        ${ingestedAt ? `<span><strong>${escapeHTML(t('catalog.meta.ingestedAt'))}:</strong> ${escapeHTML(ingestedAt)}</span>` : ''}
        ${ingestJobId ? `<span><strong>${escapeHTML(t('catalog.meta.jobId'))}:</strong> ${escapeHTML(ingestJobId)}</span>` : ''}
      </div>
    `;
    catalogResultsEl.appendChild(card);
  }

  renderCatalogPagination();
}

async function loadCatalogPage(page = 1) {
  const filters = catalogActiveFilters();
  if (filters.departmentState.department === null) {
    setCatalogFiltersExpanded(true);
    setCatalogStatusKey('catalog.validation.departmentInvalid', { error: true });
    catalogErrorMessage = t('catalog.validation.departmentInvalid');
    catalogOverviewErrorMessage = t('catalog.validation.departmentInvalid');
    renderCatalogResults();
    renderCatalogOverview();
    return;
  }

  catalogIsLoading = true;
  catalogOverviewIsLoading = true;
  catalogErrorMessage = '';
  catalogOverviewErrorMessage = '';
  setCatalogStatusKey('catalog.status.loading', { showSpinner: true });
  renderCatalogResults();
  renderCatalogOverview();
  if (catalogApplyBtn) catalogApplyBtn.disabled = true;
  if (catalogResetBtn) catalogResetBtn.disabled = true;

  try {
    const params = new URLSearchParams();
    params.set('page', String(page));
    params.set('page_size', String(CATALOG_PAGE_SIZE));
    if (filters.q) params.set('q', filters.q);
    if (filters.sourceType) params.set('source_type', filters.sourceType);
    if (filters.departmentState.department) params.set('department', filters.departmentState.department);

    const treeParams = new URLSearchParams();
    if (filters.q) treeParams.set('q', filters.q);
    if (filters.sourceType) treeParams.set('source_type', filters.sourceType);
    if (filters.departmentState.department) treeParams.set('department', filters.departmentState.department);

    const [response, treeResponse] = await Promise.all([
      fetch(apiUrl(`/catalog/sources?${params.toString()}`), { method: 'GET' }),
      fetch(apiUrl(`/catalog/tree?${treeParams.toString()}`), { method: 'GET' }),
    ]);
    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      throw new Error(formatApiError(err, response.status));
    }
    if (!treeResponse.ok) {
      const err = await treeResponse.json().catch(() => ({}));
      throw new Error(formatApiError(err, treeResponse.status));
    }

    const [payload, treePayload] = await Promise.all([
      response.json(),
      treeResponse.json(),
    ]);
    catalogModel = {
      total: Number(payload.total || 0),
      page: Number(payload.page || page),
      page_size: Number(payload.page_size || CATALOG_PAGE_SIZE),
      has_more: Boolean(payload.has_more),
      items: Array.isArray(payload.items) ? payload.items : [],
    };
    catalogOverviewModel = {
      total_groups: Number(treePayload.total_groups || 0),
      total_items: Number(treePayload.total_items || 0),
      groups: Array.isArray(treePayload.groups) ? treePayload.groups : [],
    };
    catalogHasRendered = true;
    catalogOverviewHasRendered = true;
    setCatalogStatusKey('catalog.status.loaded', {
      params: { total: catalogModel.total },
    });
  } catch (err) {
    const message = (err && typeof err === 'object' && typeof err.message === 'string')
      ? err.message
      : String(err);
    catalogErrorMessage = message;
    catalogOverviewErrorMessage = message;
    setCatalogStatusKey('catalog.status.error', {
      error: true,
      params: { message },
    });
  } finally {
    catalogIsLoading = false;
    catalogOverviewIsLoading = false;
    if (catalogApplyBtn) catalogApplyBtn.disabled = false;
    if (catalogResetBtn) catalogResetBtn.disabled = false;
    renderCatalogResults();
    renderCatalogOverview();
  }
}

function normalizeIngestSourceType(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'pdf') return 'pdf';
  if (raw === 'repo_docs') return 'repo_docs';
  return 'web';
}

function selectedIngestSourceType() {
  if (ingestSourcePdfEl?.checked) return 'pdf';
  if (ingestSourceRepoDocsEl?.checked) return 'repo_docs';
  if (ingestSourceWebEl?.checked) return 'web';
  return 'web';
}

function setIngestProgress(value) {
  if (!ingestProgressBarEl) return;
  const numeric = Number(value);
  const bounded = Number.isFinite(numeric) ? Math.max(0, Math.min(1, numeric)) : 0;
  ingestProgressBarEl.style.width = `${(bounded * 100).toFixed(1)}%`;
}

function renderIngestStatus() {
  if (!ingestStatusEl) return;
  const message = ingestStatusState.kind === 'key'
    ? t(ingestStatusState.key, ingestStatusState.params || {})
    : String(ingestStatusState.message || '');
  if (ingestStatusState.showSpinner) {
    ingestStatusEl.innerHTML = '<span class="spinner"></span>' + escapeHTML(message);
  } else {
    ingestStatusEl.textContent = message;
  }
  ingestStatusEl.className = ingestStatusState.error ? 'status error' : 'status';
}

function setIngestStatusKey(key, options = {}) {
  ingestStatusState = {
    kind: 'key',
    key,
    params: options.params || {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
    message: '',
  };
  renderIngestStatus();
}

function setIngestStatusMessage(message, options = {}) {
  ingestStatusState = {
    kind: 'message',
    key: '',
    params: {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
    message: String(message || ''),
  };
  renderIngestStatus();
}

function setPurgeProgress(value) {
  if (!purgeProgressBarEl) return;
  const numeric = Number(value);
  const bounded = Number.isFinite(numeric) ? Math.max(0, Math.min(1, numeric)) : 0;
  purgeProgressBarEl.style.width = `${(bounded * 100).toFixed(1)}%`;
}

function renderPurgeStatus() {
  if (!purgeStatusEl) return;
  const message = purgeStatusState.kind === 'key'
    ? t(purgeStatusState.key, purgeStatusState.params || {})
    : String(purgeStatusState.message || '');
  if (purgeStatusState.showSpinner) {
    purgeStatusEl.innerHTML = '<span class="spinner"></span>' + escapeHTML(message);
  } else {
    purgeStatusEl.textContent = message;
  }
  purgeStatusEl.className = purgeStatusState.error ? 'status error' : 'status';
}

function setPurgeStatusKey(key, options = {}) {
  purgeStatusState = {
    kind: 'key',
    key,
    params: options.params || {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
    message: '',
  };
  renderPurgeStatus();
}

function setPurgeStatusMessage(message, options = {}) {
  purgeStatusState = {
    kind: 'message',
    key: '',
    params: {},
    error: Boolean(options.error),
    showSpinner: Boolean(options.showSpinner),
    message: String(message || ''),
  };
  renderPurgeStatus();
}

function formatIngestStage(stage) {
  const raw = String(stage || '').trim();
  if (!raw) return '-';
  const key = `ingest.stage.${raw}`;
  const translated = t(key);
  return translated === key ? raw : translated;
}

function formatIngestState(state) {
  const raw = String(state || '').trim().toLowerCase();
  if (!raw) return '-';
  const key = `ingest.state.${raw}`;
  const translated = t(key);
  return translated === key ? raw : translated;
}

function formatPurgeStage(stage) {
  const raw = String(stage || '').trim();
  if (!raw) return '-';
  const key = `purge.stage.${raw}`;
  const translated = t(key);
  return translated === key ? raw : translated;
}

function formatPurgeState(state) {
  const raw = String(state || '').trim().toLowerCase();
  if (!raw) return '-';
  const key = `purge.state.${raw}`;
  const translated = t(key);
  return translated === key ? raw : translated;
}

function renderIngestSummary(status) {
  if (!ingestSummaryEl) return;

  const rows = [];
  rows.push(`${t('ingest.summary.jobId')}: ${status.job_id || '-'}`);
  rows.push(`${t('ingest.summary.state')}: ${formatIngestState(status.state)}`);

  if (status.message) {
    rows.push(`${t('ingest.summary.message')}: ${status.message}`);
  }

  if (status.result && typeof status.result === 'object') {
    rows.push(`${t('ingest.summary.chunkRows')}: ${status.result.chunk_rows ?? 0}`);
    rows.push(`${t('ingest.summary.deltaRows')}: ${status.result.delta_rows ?? 0}`);
    rows.push(`${t('ingest.summary.mergedRows')}: ${status.result.merged_rows ?? 0}`);
    rows.push(`${t('ingest.summary.indexUpdated')}: ${status.result.index_updated ? t('common.yes') : t('common.no')}`);
    if (status.result.output_dataset_path) {
      rows.push(`${t('ingest.summary.datasetPath')}: ${status.result.output_dataset_path}`);
    }
    if (status.result.output_delta_path) {
      rows.push(`${t('ingest.summary.deltaPath')}: ${status.result.output_delta_path}`);
    }
    if (status.result.merge_summary_path) {
      rows.push(`${t('ingest.summary.mergeSummaryPath')}: ${status.result.merge_summary_path}`);
    }
    if (status.result.index_append_summary_path) {
      rows.push(`${t('ingest.summary.appendSummaryPath')}: ${status.result.index_append_summary_path}`);
    }
    if (status.result.output_index_path) {
      rows.push(`${t('ingest.summary.indexPath')}: ${status.result.output_index_path}`);
    }
    const reloadMeta = status.result.reload_metadata;
    if (reloadMeta && typeof reloadMeta === 'object') {
      if (reloadMeta.engine_generation !== undefined && reloadMeta.engine_generation !== null) {
        rows.push(`${t('ingest.summary.reloadGeneration')}: ${reloadMeta.engine_generation}`);
      }
      if (reloadMeta.engine_loaded_at) {
        rows.push(`${t('ingest.summary.reloadLoadedAt')}: ${reloadMeta.engine_loaded_at}`);
      }
    }
  }

  if (status.error) {
    rows.push(`${t('ingest.summary.error')}: ${status.error}`);
  }

  ingestSummaryEl.innerHTML = rows.map((item) => `<div>${escapeHTML(item)}</div>`).join('');
}

function renderIngestJob(status) {
  ingestStatusModel = status;

  const stage = String(status.stage || '');
  const state = String(status.state || '').toLowerCase();
  const progress = Number(status.progress);
  const bounded = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : 0;

  setIngestProgress(bounded);
  if (ingestStageEl) {
    const pct = Math.round(bounded * 100);
    ingestStageEl.textContent = `${t('ingest.stageLabel')}: ${formatIngestStage(stage)} (${pct}%)`;
  }

  if (state === 'failed') {
    setIngestStatusKey('ingest.status.failed', { params: { jobId: status.job_id }, error: true });
  } else if (state === 'succeeded') {
    setIngestStatusKey('ingest.status.succeeded', { params: { jobId: status.job_id } });
  } else if (state === 'running') {
    setIngestStatusKey('ingest.status.running', { params: { jobId: status.job_id }, showSpinner: true });
  } else {
    setIngestStatusKey('ingest.status.queued', { params: { jobId: status.job_id }, showSpinner: true });
  }

  renderIngestSummary(status);
}

function renderPurgeSummary(status) {
  if (!purgeSummaryEl) return;

  const rows = [];
  rows.push(`${t('purge.summary.jobId')}: ${status.job_id || '-'}`);
  rows.push(`${t('purge.summary.state')}: ${formatPurgeState(status.state)}`);
  if (status.message) {
    rows.push(`${t('purge.summary.message')}: ${status.message}`);
  }

  const request = status.request && typeof status.request === 'object'
    ? status.request
    : {};
  const result = status.result && typeof status.result === 'object'
    ? status.result
    : null;
  const stageMetrics = result && typeof result.stage_metrics === 'object'
    ? result.stage_metrics
    : {};
  const purgeMetric = stageMetrics && typeof stageMetrics.purge_dataset === 'object'
    ? stageMetrics.purge_dataset
    : {};
  const rebuildMetric = stageMetrics && typeof stageMetrics.full_rebuild === 'object'
    ? stageMetrics.full_rebuild
    : {};
  const reloadMetric = stageMetrics && typeof stageMetrics.reload === 'object'
    ? stageMetrics.reload
    : {};

  const targetDepartment = purgeMetric.target_department || request.department || '-';
  rows.push(`${t('purge.summary.targetDepartment')}: ${targetDepartment}`);

  const dryRun = typeof purgeMetric.dry_run === 'boolean'
    ? purgeMetric.dry_run
    : Boolean(request.dry_run);
  rows.push(`${t('purge.summary.dryRun')}: ${dryRun ? t('common.yes') : t('common.no')}`);

  if (purgeMetric.rows_before !== undefined && purgeMetric.rows_before !== null) {
    rows.push(`${t('purge.summary.rowsBefore')}: ${purgeMetric.rows_before}`);
  }
  const rowsRemoved = purgeMetric.rows_removed ?? result?.delta_rows ?? 0;
  rows.push(`${t('purge.summary.rowsRemoved')}: ${rowsRemoved}`);
  if (purgeMetric.rows_after !== undefined && purgeMetric.rows_after !== null) {
    rows.push(`${t('purge.summary.rowsAfter')}: ${purgeMetric.rows_after}`);
  } else if (result && result.merged_rows !== undefined && result.merged_rows !== null) {
    rows.push(`${t('purge.summary.rowsAfter')}: ${result.merged_rows}`);
  }

  const datasetPath = purgeMetric.output_dataset_path || result?.output_dataset_path;
  if (datasetPath) {
    rows.push(`${t('purge.summary.datasetPath')}: ${datasetPath}`);
  }
  const purgeSummaryPath = result?.purge_summary_path || purgeMetric.summary_path;
  if (purgeSummaryPath) {
    rows.push(`${t('purge.summary.purgeSummaryPath')}: ${purgeSummaryPath}`);
  }
  const backupDatasetPath = result?.backup_dataset_path || purgeMetric.backup_dataset_path;
  if (backupDatasetPath) {
    rows.push(`${t('purge.summary.backupDatasetPath')}: ${backupDatasetPath}`);
  }

  if (Object.keys(rebuildMetric).length > 0 || result?.full_rebuild_summary_path) {
    const rebuildStatus = rebuildMetric.status || '-';
    rows.push(`${t('purge.summary.rebuildStatus')}: ${rebuildStatus}`);

    if (rebuildMetric.duration_seconds !== undefined && rebuildMetric.duration_seconds !== null) {
      rows.push(`${t('purge.summary.rebuildDuration')}: ${rebuildMetric.duration_seconds}`);
    }

    const rebuildIndexPath = rebuildMetric.index_path || result?.output_index_path;
    if (rebuildIndexPath) {
      rows.push(`${t('purge.summary.rebuildIndexPath')}: ${rebuildIndexPath}`);
    }
    if (rebuildMetric.vector_count !== undefined && rebuildMetric.vector_count !== null) {
      rows.push(`${t('purge.summary.rebuildVectorCount')}: ${rebuildMetric.vector_count}`);
    }
    if (rebuildMetric.doc_count !== undefined && rebuildMetric.doc_count !== null) {
      rows.push(`${t('purge.summary.rebuildDocCount')}: ${rebuildMetric.doc_count}`);
    }
    const rebuildSummaryPath = result?.full_rebuild_summary_path || rebuildMetric.summary_path;
    if (rebuildSummaryPath) {
      rows.push(`${t('purge.summary.rebuildSummaryPath')}: ${rebuildSummaryPath}`);
    }
  }

  const reloadMeta = result && typeof result.reload_metadata === 'object'
    ? result.reload_metadata
    : null;
  const reloadSource = reloadMeta || reloadMetric;
  if (reloadSource && typeof reloadSource === 'object') {
    if (reloadSource.engine_generation !== undefined && reloadSource.engine_generation !== null) {
      rows.push(`${t('purge.summary.reloadGeneration')}: ${reloadSource.engine_generation}`);
    }
    if (reloadSource.engine_loaded_at) {
      rows.push(`${t('purge.summary.reloadLoadedAt')}: ${reloadSource.engine_loaded_at}`);
    }
  }

  if (status.error) {
    rows.push(`${t('purge.summary.error')}: ${status.error}`);
  }
  if (rebuildMetric.error) {
    rows.push(`${t('purge.summary.error')}: ${rebuildMetric.error}`);
  }

  purgeSummaryEl.innerHTML = rows.map((item) => `<div>${escapeHTML(item)}</div>`).join('');
}

function renderPurgeJob(status) {
  purgeStatusModel = status;

  const stage = String(status.stage || '');
  const state = String(status.state || '').toLowerCase();
  const progress = Number(status.progress);
  const bounded = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : 0;

  setPurgeProgress(bounded);
  if (purgeStageEl) {
    const pct = Math.round(bounded * 100);
    purgeStageEl.textContent = `${t('purge.stageLabel')}: ${formatPurgeStage(stage)} (${pct}%)`;
  }

  if (state === 'failed') {
    setPurgeStatusKey('purge.status.failed', { params: { jobId: status.job_id }, error: true });
  } else if (state === 'succeeded') {
    setPurgeStatusKey('purge.status.succeeded', { params: { jobId: status.job_id } });
  } else if (state === 'running') {
    setPurgeStatusKey('purge.status.running', { params: { jobId: status.job_id }, showSpinner: true });
  } else {
    setPurgeStatusKey('purge.status.queued', { params: { jobId: status.job_id }, showSpinner: true });
  }

  renderPurgeSummary(status);
}

function syncIngestSourceVisibility() {
  const sourceType = selectedIngestSourceType();
  const isWeb = sourceType === 'web';
  const isPdf = sourceType === 'pdf';
  const isRepoDocs = sourceType === 'repo_docs';
  if (ingestWebFieldsEl) ingestWebFieldsEl.hidden = !isWeb;
  if (ingestPdfFieldsEl) ingestPdfFieldsEl.hidden = !isPdf;
  if (ingestRepoDocsFieldsEl) ingestRepoDocsFieldsEl.hidden = !isRepoDocs;
  if (ingestUrlEl) ingestUrlEl.required = isWeb;
  if (ingestFileEl) ingestFileEl.required = isPdf;
  if (ingestRepoUrlEl) ingestRepoUrlEl.required = isRepoDocs;
}

function stopIngestPolling() {
  if (ingestPollTimer) {
    clearTimeout(ingestPollTimer);
    ingestPollTimer = null;
  }
}

function stopPurgePolling() {
  if (purgePollTimer) {
    clearTimeout(purgePollTimer);
    purgePollTimer = null;
  }
}

function isTerminalJobState(state) {
  const raw = String(state || '').toLowerCase();
  return raw === 'succeeded' || raw === 'failed';
}

async function pollIngestJob(jobId) {
  stopIngestPolling();

  const runPoll = async () => {
    try {
      const response = await fetch(apiUrl(`/ingest/jobs/${encodeURIComponent(jobId)}`), { method: 'GET' });
      if (!response.ok) {
        if (response.status === 404) {
          ingestActiveJobId = null;
          setIngestStatusKey('ingest.error.jobNotFound', {
            error: true,
            params: { jobId },
          });
          if (ingestSubmitBtn) ingestSubmitBtn.disabled = false;
          return;
        }
        const err = await response.json().catch(() => ({}));
        throw new Error(formatApiError(err, response.status));
      }

      const status = await response.json();
      renderIngestJob(status);

      if (isTerminalJobState(status.state)) {
        if (String(status.state || '').toLowerCase() === 'succeeded') {
          void loadSearchDepartments();
        }
        ingestActiveJobId = null;
        if (ingestSubmitBtn) ingestSubmitBtn.disabled = false;
        return;
      }

      ingestPollTimer = window.setTimeout(runPoll, 1500);
    } catch (err) {
      const message = (err && typeof err === 'object' && typeof err.message === 'string')
        ? err.message
        : String(err);
      setIngestStatusKey('ingest.error.poll', {
        error: true,
        params: { message },
      });
      ingestPollTimer = window.setTimeout(runPoll, 2500);
    }
  };

  await runPoll();
}

async function pollPurgeJob(jobId) {
  stopPurgePolling();

  const runPoll = async () => {
    try {
      const response = await fetch(apiUrl(`/ingest/jobs/${encodeURIComponent(jobId)}`), { method: 'GET' });
      if (!response.ok) {
        if (response.status === 404) {
          purgeActiveJobId = null;
          setPurgeStatusKey('purge.error.jobNotFound', {
            error: true,
            params: { jobId },
          });
          if (purgeSubmitBtn) purgeSubmitBtn.disabled = false;
          return;
        }
        const err = await response.json().catch(() => ({}));
        throw new Error(formatApiError(err, response.status));
      }

      const status = await response.json();
      renderPurgeJob(status);

      if (isTerminalJobState(status.state)) {
        if (String(status.state || '').toLowerCase() === 'succeeded') {
          void loadSearchDepartments();
        }
        purgeActiveJobId = null;
        if (purgeSubmitBtn) purgeSubmitBtn.disabled = false;
        return;
      }

      purgePollTimer = window.setTimeout(runPoll, 1500);
    } catch (err) {
      const message = (err && typeof err === 'object' && typeof err.message === 'string')
        ? err.message
        : String(err);
      setPurgeStatusKey('purge.error.poll', {
        error: true,
        params: { message },
      });
      purgePollTimer = window.setTimeout(runPoll, 2500);
    }
  };

  await runPoll();
}

function normalizeIngestDepartmentRaw(value) {
  let raw = String(value || '');
  if (typeof raw.normalize === 'function') {
    raw = raw.normalize('NFKD').replace(/[\u0300-\u036f]/g, '');
  }
  raw = raw.replace(/[^\x00-\x7F]/g, '');
  return raw.trim().toLowerCase();
}

function sanitizeIngestDepartment(value) {
  const raw = normalizeIngestDepartmentRaw(value);
  if (!raw) return '';
  return raw
    .replace(/[\s/]+/g, '_')
    .replace(/[^a-z0-9_-]/g, '')
    .replace(/[_-]{2,}/g, '_')
    .replace(/^[_-]+|[_-]+$/g, '');
}

function validIngestDepartment(value) {
  const raw = normalizeIngestDepartmentRaw(value);
  if (!raw) {
    return { department: null, reason: 'required' };
  }
  if (INGEST_DEPARTMENT_UNSAFE_RE.test(raw)) {
    return { department: null, reason: 'invalid' };
  }

  const normalized = sanitizeIngestDepartment(raw);
  if (!normalized) {
    return { department: null, reason: 'invalid' };
  }
  if (normalized.length < INGEST_DEPARTMENT_MIN_LENGTH || normalized.length > INGEST_DEPARTMENT_MAX_LENGTH) {
    return { department: null, reason: 'invalid' };
  }
  if (!INGEST_DEPARTMENT_FINAL_RE.test(normalized)) {
    return { department: null, reason: 'invalid' };
  }
  return { department: normalized, reason: null };
}

function normalizeIngestLabel(value) {
  return String(value || '').trim();
}

function validIngestLabel(value) {
  const label = normalizeIngestLabel(value);
  if (!label) {
    return { label: null, reason: null };
  }
  if (label.length > INGEST_LABEL_MAX_LENGTH || !INGEST_LABEL_RE.test(label)) {
    return { label: null, reason: 'invalid' };
  }
  return { label, reason: null };
}

function isLikelyPdfFile(file) {
  if (!file) return false;
  const type = String(file.type || '').toLowerCase();
  const name = String(file.name || '').toLowerCase();
  if (type === 'application/pdf' || type === 'application/x-pdf') return true;
  if (type === 'application/octet-stream' || type === 'binary/octet-stream') {
    return name.endsWith('.pdf');
  }
  return name.endsWith('.pdf');
}

function rerankClass(score) {
  const numeric = Number(score);
  if (!Number.isFinite(numeric)) return 'rerank-badge rerank-badge-yellow';
  if (numeric < 0) return 'rerank-badge rerank-badge-red';
  if (numeric < 6) return 'rerank-badge rerank-badge-yellow';
  return 'rerank-badge rerank-badge-green';
}

function paginationItems(totalPages, page) {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, i) => i + 1);
  }

  const pages = [1];
  let start = Math.max(2, page - 1);
  let end = Math.min(totalPages - 1, page + 1);
  if (page <= 3) {
    start = 2;
    end = 4;
  } else if (page >= totalPages - 2) {
    start = totalPages - 3;
    end = totalPages - 1;
  }

  if (start > 2) pages.push('...');
  for (let p = start; p <= end; p += 1) pages.push(p);
  if (end < totalPages - 1) pages.push('...');
  pages.push(totalPages);
  return pages;
}

function renderPagination(total, page) {
  if (!paginationEl) return;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  if (totalPages <= 1) {
    paginationEl.innerHTML = '';
    return;
  }

  const start = (page - 1) * PAGE_SIZE + 1;
  const end = Math.min(page * PAGE_SIZE, total);
  const items = paginationItems(totalPages, page);
  const controls = items.map((item) => {
    if (item === '...') {
      return '<span class="pagination-ellipsis">...</span>';
    }
    const activeClass = item === page ? ' is-active' : '';
    return `<button class="page-btn${activeClass}" data-page="${item}" type="button">${item}</button>`;
  }).join('');

  paginationEl.innerHTML = `
    <div class="pagination-summary">${escapeHTML(t('results.summary', { start, end, total }))}</div>
    <div class="pagination-controls">
      <button class="page-btn" type="button" data-page="${page - 1}" ${page <= 1 ? 'disabled' : ''}>${escapeHTML(t('pagination.prev'))}</button>
      ${controls}
      <button class="page-btn" type="button" data-page="${page + 1}" ${page >= totalPages ? 'disabled' : ''}>${escapeHTML(t('pagination.next'))}</button>
    </div>
  `;
}

function formatDepartmentDisplay(value) {
  const canonical = canonicalDepartment(value);
  if (canonical === 'aplicaciones') return t('department.option.aplicaciones');
  if (canonical === 'sistemas') return t('department.option.sistemas');
  if (canonical === 'bigdata') return t('department.option.bigdata');
  if (canonical === 'slurm') return t('department.option.slurm');
  if (canonical === 'all') return t('department.option.all');

  const raw = value == null ? '' : String(value).trim();
  return raw || '-';
}

function inferResultSourceKind(row) {
  const rawType = String(row?.source_type || '').trim().toLowerCase();
  if (rawType === 'html') return 'web';
  if (rawType === 'pdf') return 'pdf';

  const rawSource = String(row?.source || '').trim().toLowerCase();
  if (/\.pdf(?:$|[?#])/.test(rawSource)) return 'pdf';
  return 'ticket';
}

function renderCurrentPage() {
  if (!hasRenderedResults) {
    resultsEl.innerHTML = '';
    if (paginationEl) paginationEl.innerHTML = '';
    return;
  }

  resultsEl.innerHTML = '';
  if (!currentResults.length) {
    resultsEl.innerHTML = `
      <div class="empty-state">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#8ea0b8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
          <line x1="8" y1="11" x2="14" y2="11"/>
        </svg>
        <p>${escapeHTML(t('results.none'))}</p>
      </div>`;
    if (paginationEl) paginationEl.innerHTML = '';
    return;
  }

  const total = currentResults.length;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  if (currentPage > totalPages) currentPage = totalPages;
  const pageStart = (currentPage - 1) * PAGE_SIZE;
  const pageRows = currentResults.slice(pageStart, pageStart + PAGE_SIZE);

  for (const [index, row] of pageRows.entries()) {
    const card = document.createElement('article');
    card.className = 'card';
    card.style.setProperty('--card-index', String(index));
    const sourceKind = inferResultSourceKind(row);
    const safeSource = safeLinkURL(row.source);
    const sourceLabelKey = sourceKind === 'web'
      ? 'card.openWeb'
      : sourceKind === 'pdf'
        ? 'card.openDocument'
        : 'card.openTicket';
    const sourceLink = safeSource
      ? `<a href="${escapeHTML(safeSource)}" target="_blank" rel="noopener">${escapeHTML(t(sourceLabelKey))}</a>`
      : '';
    const showLastUpdate = sourceKind === 'ticket';
    const lastUpdatedText = row.last_updated && String(row.last_updated).trim()
      ? escapeHTML(row.last_updated)
      : escapeHTML(t('card.noDate'));
    const rank = row.rank ?? (pageStart + index + 1);
    const rerankScore = Number(row.rerank_score);
    const fusedScore = Number(row.fused_score);
    const rerankText = Number.isFinite(rerankScore) ? rerankScore.toFixed(4) : '-';
    const fusedText = Number.isFinite(fusedScore) ? fusedScore.toFixed(6) : '-';
    const departmentText = formatDepartmentDisplay(row.department);

    card.innerHTML = `
      <div class="row">
        <strong class="rank-label">#${escapeHTML(rank)}</strong>
        <span class="${rerankClass(rerankScore)}">${escapeHTML(t('card.rerank'))}: ${escapeHTML(rerankText)}</span>
        <span class="fused-label">${escapeHTML(t('card.fused'))}: ${escapeHTML(fusedText)}</span>
      </div>
      <div class="meta">
        <span>${escapeHTML(t('card.conversation'))}: ${escapeHTML(row.conversation_id ?? '-')}</span>
        <span>${escapeHTML(t('card.ticket'))}: ${escapeHTML(row.ticket_id ?? '-')}</span>
        <span>${escapeHTML(t('card.chunk'))}: ${escapeHTML(row.chunk_id ?? '-')}</span>
        ${showLastUpdate ? `<span>${escapeHTML(t('card.lastUpdate'))}: ${lastUpdatedText}</span>` : ''}
        <span class="dept-badge">${escapeHTML(t('card.department'))}: ${escapeHTML(departmentText)}</span>
        ${sourceLink}
      </div>
      <div class="snippet-box"><p>${escapeHTML(row.snippet)}</p></div>
    `;
    resultsEl.appendChild(card);
  }

  renderPagination(total, currentPage);
}

function setResultDataset(results) {
  currentResults = Array.isArray(results) ? results : [];
  currentPage = 1;
  hasRenderedResults = true;
  renderCurrentPage();
}

function clearDateFilters() {
  if (dateFromPicker && dateToPicker) {
    dateFromPicker.clear();
    dateToPicker.clear();
    dateFromPicker.set('maxDate', null);
    dateToPicker.set('minDate', null);
    return;
  }
  dateFromEl.value = '';
  dateToEl.value = '';
}

function getDatePickerLocale(language) {
  if (language === 'es') return FLATPICKR_LOCALES.es;
  if (language === 'gl') return FLATPICKR_LOCALES.gl;
  return 'default';
}

function updateDateInputPlaceholders() {
  const placeholder = t('form.noDateSelected');
  if (dateFromEl) dateFromEl.placeholder = placeholder;
  if (dateToEl) dateToEl.placeholder = placeholder;
  if (dateFromPicker?.altInput) dateFromPicker.altInput.placeholder = placeholder;
  if (dateToPicker?.altInput) dateToPicker.altInput.placeholder = placeholder;
}

function refreshDatePickerLocale() {
  if (!dateFromPicker || !dateToPicker) {
    updateDateInputPlaceholders();
    return;
  }

  const locale = getDatePickerLocale(currentLanguage);
  dateFromPicker.set('locale', locale);
  dateToPicker.set('locale', locale);

  syncCustomMonthDropdown(dateFromPicker);
  syncCustomMonthDropdown(dateToPicker);
  updateDateInputPlaceholders();
}

function initDatePickers() {
  if (typeof window.flatpickr !== 'function') {
    updateDateInputPlaceholders();
    return;
  }

  const baseConfig = {
    dateFormat: 'Y-m-d',
    altInput: true,
    altFormat: 'd/m/Y',
    allowInput: false,
    disableMobile: true,
    monthSelectorType: 'dropdown',
    locale: getDatePickerLocale(currentLanguage),
  };

  dateFromPicker = window.flatpickr(dateFromEl, {
    ...baseConfig,
    onChange: (selectedDates) => {
      const min = selectedDates[0] || null;
      if (dateToPicker) {
        dateToPicker.set('minDate', min);
      }
    },
    onReady: (_, __, fp) => syncCustomMonthDropdown(fp),
    onMonthChange: (_, __, fp) => syncCustomMonthDropdown(fp),
    onYearChange: (_, __, fp) => syncCustomMonthDropdown(fp),
    onOpen: (_, __, fp) => syncCustomMonthDropdown(fp),
    onClose: (_, __, fp) => closeMonthMenu(fp),
  });

  dateToPicker = window.flatpickr(dateToEl, {
    ...baseConfig,
    onChange: (selectedDates) => {
      const max = selectedDates[0] || null;
      if (dateFromPicker) {
        dateFromPicker.set('maxDate', max);
      }
    },
    onReady: (_, __, fp) => syncCustomMonthDropdown(fp),
    onMonthChange: (_, __, fp) => syncCustomMonthDropdown(fp),
    onYearChange: (_, __, fp) => syncCustomMonthDropdown(fp),
    onOpen: (_, __, fp) => syncCustomMonthDropdown(fp),
    onClose: (_, __, fp) => closeMonthMenu(fp),
  });

  syncCustomMonthDropdown(dateFromPicker);
  syncCustomMonthDropdown(dateToPicker);
  updateDateInputPlaceholders();
}

function applyTranslations() {
  document.documentElement.lang = currentLanguage;

  for (const node of document.querySelectorAll('[data-i18n]')) {
    const key = node.getAttribute('data-i18n');
    if (!key) continue;
    node.textContent = t(key);
  }

  for (const node of document.querySelectorAll('[data-i18n-placeholder]')) {
    const key = node.getAttribute('data-i18n-placeholder');
    if (!key) continue;
    node.placeholder = t(key);
  }

  for (const node of document.querySelectorAll('[data-i18n-content]')) {
    const key = node.getAttribute('data-i18n-content');
    if (!key) continue;
    node.setAttribute('content', t(key));
  }

  refreshDatePickerLocale();
  setLanguageValue(currentLanguage);
  renderDepartmentOptions(availableSearchDepartments);
  setCatalogSourceTypeValue(catalogSourceTypeEl?.value || '');
  renderStatus();
  syncIngestSourceVisibility();
  if (ingestStatusModel) {
    renderIngestJob(ingestStatusModel);
  } else {
    renderIngestStatus();
  }
  if (purgeStatusModel) {
    renderPurgeJob(purgeStatusModel);
  } else {
    renderPurgeStatus();
  }
  renderWorkspaceTabs();
  renderCatalogViewTabs();
  renderCatalogFiltersDrawer();
  renderCatalogStatus();
  renderCatalogOverview();
  renderCatalogResults();

  if (hasRenderedResults) {
    renderCurrentPage();
  }
}

function initCursorBackgroundAura() {
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const coarsePointer = window.matchMedia('(pointer: coarse)').matches;
  if (prefersReducedMotion || coarsePointer) return;

  let rafId = null;
  let nextX = window.innerWidth * 0.5;
  let nextY = window.innerHeight * 0.34;
  let active = false;

  const rootStyle = document.documentElement.style;
  rootStyle.setProperty('--cursor-x', `${nextX}px`);
  rootStyle.setProperty('--cursor-y', `${nextY}px`);
  rootStyle.setProperty('--cursor-glow-opacity', '0');

  const flush = () => {
    rafId = null;
    rootStyle.setProperty('--cursor-x', `${nextX}px`);
    rootStyle.setProperty('--cursor-y', `${nextY}px`);
  };

  const schedule = () => {
    if (rafId !== null) return;
    rafId = window.requestAnimationFrame(flush);
  };

  const onPointerMove = (ev) => {
    nextX = ev.clientX;
    nextY = ev.clientY;
    if (!active) {
      active = true;
      rootStyle.setProperty('--cursor-glow-opacity', '1');
    }
    schedule();
  };

  document.addEventListener('pointermove', onPointerMove, { passive: true });

  window.addEventListener('blur', () => {
    active = false;
    rootStyle.setProperty('--cursor-glow-opacity', '0');
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      active = false;
      rootStyle.setProperty('--cursor-glow-opacity', '0');
    }
  });
}

form.addEventListener('submit', async (ev) => {
  ev.preventDefault();
  if (isSubmitting) return;

  const query = queryEl.value.trim();
  const department = normalizeDepartmentValue(departmentEl?.value || 'all');
  const k = Number(kEl.value === '' ? '8' : kEl.value);
  const dateFrom = toApiDate(dateFromEl.value || null);
  const dateTo = toApiDate(dateToEl.value || null);
  const hasDateFilter = Boolean(dateFrom || dateTo);

  if (!Number.isInteger(k) || k < 0 || k > 20) {
    setStatusKey('validation.kRange', { error: true });
    return;
  }
  if (!query && !hasDateFilter) {
    setStatusKey('validation.queryOrDate', { error: true });
    return;
  }
  if (query && k === 0) {
    setStatusKey('validation.kZeroDateOnly', { error: true });
    return;
  }
  if (isCanonicalDate(dateFrom) && isCanonicalDate(dateTo) && dateFrom > dateTo) {
    setStatusKey('validation.invalidRange', { error: true });
    return;
  }

  isSubmitting = true;
  setWorkspaceTab('search');
  searchBtn.disabled = true;
  hasRenderedResults = false;
  setStatusKey('status.searching', { showSpinner: true });
  setLoadingState(true);
  resultsEl.innerHTML = '';
  if (paginationEl) paginationEl.innerHTML = '';

  try {
    const payload = { query, k, department };
    if (dateFrom) payload.date_from = dateFrom;
    if (dateTo) payload.date_to = dateTo;

    const response = await fetch(apiUrl('/search'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      if (response.status === 503) {
        throw new Error(t('error.engineWarmup'));
      }
      throw new Error(formatApiError(err, response.status));
    }

    const data = await response.json();
    setStatusKey('status.done', { params: { total: data.total } });
    setResultDataset(data.results);
  } catch (err) {
    const message = (err && typeof err === 'object' && typeof err.message === 'string')
      ? err.message
      : String(err);
    setStatusKey('error.prefix', {
      error: true,
      params: { message },
    });
  } finally {
    isSubmitting = false;
    searchBtn.disabled = false;
  }
});

clearBtn.addEventListener('click', () => {
  queryEl.value = '';
  setDepartmentValue('all');
  clearDateFilters();
  currentResults = [];
  currentPage = 1;
  hasRenderedResults = false;
  resultsEl.innerHTML = '';
  if (paginationEl) paginationEl.innerHTML = '';
  setStatusKey('status.ready');
  queryEl.focus();
});

if (clearDatesBtn) {
  clearDatesBtn.addEventListener('click', () => {
    clearDateFilters();
    setStatusKey('status.dateCleared');
  });
}

if (ingestForm) {
  const onIngestSourceChange = () => {
    syncIngestSourceVisibility();
  };
  ingestSourceWebEl?.addEventListener('change', onIngestSourceChange);
  ingestSourcePdfEl?.addEventListener('change', onIngestSourceChange);
  ingestSourceRepoDocsEl?.addEventListener('change', onIngestSourceChange);
  syncIngestSourceVisibility();

  ingestForm.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    if (ingestIsSubmitting) return;
    if (ingestActiveJobId) {
      setIngestStatusKey('ingest.validation.jobActive', {
        error: true,
        params: { jobId: ingestActiveJobId },
      });
      return;
    }

    const sourceType = normalizeIngestSourceType(selectedIngestSourceType());
    const departmentCheck = validIngestDepartment(ingestDepartmentEl?.value || '');
    const department = departmentCheck.department;
    const labelCheck = validIngestLabel(ingestLabelEl?.value || '');
    const ingestLabel = labelCheck.label;

    if (!department) {
      setIngestStatusKey(
        departmentCheck.reason === 'required'
          ? 'ingest.validation.departmentRequired'
          : 'ingest.validation.departmentInvalid',
        { error: true },
      );
      ingestDepartmentEl?.focus();
      return;
    }
    if (ingestDepartmentEl) ingestDepartmentEl.value = department;
    if (labelCheck.reason === 'invalid') {
      setIngestStatusKey('ingest.validation.ingestLabelInvalid', { error: true });
      ingestLabelEl?.focus();
      return;
    }
    if (ingestLabelEl) ingestLabelEl.value = ingestLabel || '';

    const rawUrl = String(ingestUrlEl?.value || '').trim();
    const repoDocsRawUrl = String(ingestRepoUrlEl?.value || '').trim();
    const url = safeLinkURL(rawUrl);
    const repoDocsUrl = safeLinkURL(repoDocsRawUrl);
    const file = ingestFileEl?.files && ingestFileEl.files.length > 0
      ? ingestFileEl.files[0]
      : null;

    if (sourceType === 'web' && !url) {
      setIngestStatusKey('ingest.validation.urlRequired', { error: true });
      ingestUrlEl?.focus();
      return;
    }
    if (sourceType === 'repo_docs' && !repoDocsUrl) {
      setIngestStatusKey('ingest.validation.repoDocsUrlRequired', { error: true });
      ingestRepoUrlEl?.focus();
      return;
    }
    if (sourceType === 'pdf' && !file) {
      setIngestStatusKey('ingest.validation.pdfRequired', { error: true });
      ingestFileEl?.focus();
      return;
    }
    if (sourceType === 'pdf' && file && !isLikelyPdfFile(file)) {
      setIngestStatusKey('ingest.validation.pdfInvalidType', { error: true });
      ingestFileEl?.focus();
      return;
    }
    if (sourceType === 'pdf' && file && Number(file.size || 0) > MAX_CLIENT_PDF_UPLOAD_BYTES) {
      setIngestStatusKey('ingest.validation.pdfTooLarge', {
        error: true,
        params: { maxMB: Math.round(MAX_CLIENT_PDF_UPLOAD_BYTES / (1024 * 1024)) },
      });
      ingestFileEl?.focus();
      return;
    }

    ingestIsSubmitting = true;
    if (ingestSubmitBtn) ingestSubmitBtn.disabled = true;
    setIngestStatusKey('ingest.status.submitting', { showSpinner: true });
    if (ingestStageEl) ingestStageEl.textContent = '';
    if (ingestSummaryEl) ingestSummaryEl.textContent = '';
    setIngestProgress(0);

    try {
      let response;
      if (sourceType === 'web') {
        response = await fetch(apiUrl('/ingest/web'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            url,
            department,
            ...(ingestLabel ? { ingest_label: ingestLabel } : {}),
          }),
        });
      } else if (sourceType === 'repo_docs') {
        response = await fetch(apiUrl('/ingest/repo-docs'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            url: repoDocsUrl,
            department,
            ...(ingestLabel ? { ingest_label: ingestLabel } : {}),
          }),
        });
      } else {
        const payload = new FormData();
        payload.set('department', department);
        if (ingestLabel) payload.set('ingest_label', ingestLabel);
        payload.set('file', file);
        response = await fetch(apiUrl('/ingest/pdf'), {
          method: 'POST',
          body: payload,
        });
      }

      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(formatApiError(err, response.status));
      }

      const status = await response.json();
      ingestActiveJobId = status.job_id;
      renderIngestJob(status);
      await pollIngestJob(status.job_id);
    } catch (err) {
      const message = (err && typeof err === 'object' && typeof err.message === 'string')
        ? err.message
        : String(err);
      setIngestStatusMessage(t('error.prefix', { message }), { error: true });
    } finally {
      ingestIsSubmitting = false;
      if (!ingestActiveJobId && ingestSubmitBtn) {
        ingestSubmitBtn.disabled = false;
      }
    }
  });
}

if (purgeForm) {
  purgeForm.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    if (purgeIsSubmitting) return;
    if (purgeActiveJobId) {
      setPurgeStatusKey('purge.validation.jobActive', {
        error: true,
        params: { jobId: purgeActiveJobId },
      });
      return;
    }

    const departmentCheck = validIngestDepartment(purgeDepartmentEl?.value || '');
    const department = departmentCheck.department;
    if (!department) {
      setPurgeStatusKey(
        departmentCheck.reason === 'required'
          ? 'purge.validation.departmentRequired'
          : 'purge.validation.departmentInvalid',
        { error: true },
      );
      purgeDepartmentEl?.focus();
      return;
    }
    if (purgeDepartmentEl) purgeDepartmentEl.value = department;

    if (!purgeConfirmEl?.checked) {
      setPurgeStatusKey('purge.validation.confirmRequired', { error: true });
      purgeConfirmEl?.focus();
      return;
    }

    const dryRun = Boolean(purgeDryRunEl?.checked);
    const confirmKey = dryRun ? 'purge.confirm.dialogDryRun' : 'purge.confirm.dialog';
    if (!window.confirm(t(confirmKey, { department }))) {
      setPurgeStatusKey('purge.validation.cancelled', { error: true });
      return;
    }

    purgeIsSubmitting = true;
    if (purgeSubmitBtn) purgeSubmitBtn.disabled = true;
    setPurgeStatusKey('purge.status.submitting', { showSpinner: true });
    if (purgeStageEl) purgeStageEl.textContent = '';
    if (purgeSummaryEl) purgeSummaryEl.textContent = '';
    setPurgeProgress(0);

    try {
      const response = await fetch(apiUrl('/admin/purge-department'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          department,
          confirm: true,
          dry_run: dryRun,
        }),
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(formatApiError(err, response.status));
      }

      const status = await response.json();
      purgeActiveJobId = status.job_id;
      if (purgeConfirmEl) purgeConfirmEl.checked = false;
      renderPurgeJob(status);
      await pollPurgeJob(status.job_id);
    } catch (err) {
      const message = (err && typeof err === 'object' && typeof err.message === 'string')
        ? err.message
        : String(err);
      setPurgeStatusMessage(t('error.prefix', { message }), { error: true });
    } finally {
      purgeIsSubmitting = false;
      if (!purgeActiveJobId && purgeSubmitBtn) {
        purgeSubmitBtn.disabled = false;
      }
    }
  });
}

if (catalogForm) {
  catalogForm.addEventListener('submit', async (ev) => {
    ev.preventDefault();
    setWorkspaceTab('catalog');
    setCatalogView('overview');
    await loadCatalogPage(1);
  });
}

if (catalogResetBtn) {
  catalogResetBtn.addEventListener('click', () => {
    if (catalogQueryEl) catalogQueryEl.value = '';
    if (catalogSourceTypeEl) catalogSourceTypeEl.value = '';
    if (catalogDepartmentEl) catalogDepartmentEl.value = '';
    setWorkspaceTab('catalog');
    setCatalogView('overview');
    void loadCatalogPage(1);
  });
}

if (catalogFiltersToggleEl) {
  catalogFiltersToggleEl.addEventListener('click', () => {
    setCatalogFiltersExpanded(!catalogFiltersExpanded, {
      focusPanel: !catalogFiltersExpanded,
    });
  });
}

if (adminOpenCatalogBtn) {
  adminOpenCatalogBtn.addEventListener('click', () => {
    if (adminToolsDisclosureEl) {
      adminToolsDisclosureEl.open = true;
    }
    setWorkspaceTab('catalog');
    setCatalogView('overview');
    void loadCatalogPage(1);
  });
}

for (const input of [catalogQueryEl, catalogSourceTypeEl, catalogDepartmentEl]) {
  input?.addEventListener('input', () => {
    renderCatalogFiltersDrawer();
  });
  input?.addEventListener('change', () => {
    renderCatalogFiltersDrawer();
  });
}

if (paginationEl) {
  paginationEl.addEventListener('click', (ev) => {
    const btn = ev.target.closest('.page-btn[data-page]');
    if (!btn || btn.disabled) return;
    const targetPage = Number(btn.dataset.page);
    const totalPages = Math.max(1, Math.ceil(currentResults.length / PAGE_SIZE));
    if (!Number.isInteger(targetPage) || targetPage < 1 || targetPage > totalPages) return;
    currentPage = targetPage;
    renderCurrentPage();
  });
}

if (catalogPaginationEl) {
  catalogPaginationEl.addEventListener('click', (ev) => {
    const btn = ev.target.closest('.page-btn[data-page]');
    if (!btn || btn.disabled || catalogIsLoading) return;
    const targetPage = Number(btn.dataset.page);
    const totalPages = Math.max(1, Math.ceil(Number(catalogModel.total || 0) / Number(catalogModel.page_size || CATALOG_PAGE_SIZE)));
    if (!Number.isInteger(targetPage) || targetPage < 1 || targetPage > totalPages) return;
    setWorkspaceTab('catalog');
    setCatalogView('browse');
    void loadCatalogPage(targetPage);
  });
}

for (const trigger of [catalogViewTabOverviewEl, catalogViewTabBrowseEl]) {
  trigger?.addEventListener('click', () => {
    setCatalogView(trigger.dataset.view || 'overview');
  });
}

for (const trigger of [workspaceTabSearchEl, workspaceTabCatalogEl]) {
  trigger?.addEventListener('click', () => {
    setWorkspaceTab(trigger.dataset.tab || 'search');
  });
}

document.addEventListener('keydown', (ev) => {
  const inCatalogFilters = ev.target === catalogFiltersToggleEl
    || catalogFiltersPanelEl?.contains(ev.target);
  if (ev.key === 'Escape' && inCatalogFilters && catalogFiltersExpanded) {
    ev.preventDefault();
    setCatalogFiltersExpanded(false, { focusTrigger: true });
    return;
  }

  const isWorkspaceTab = ev.target === workspaceTabSearchEl || ev.target === workspaceTabCatalogEl;
  const isCatalogViewTab = ev.target === catalogViewTabOverviewEl || ev.target === catalogViewTabBrowseEl;
  if (!isWorkspaceTab && !isCatalogViewTab) return;
  if (ev.key !== 'ArrowLeft' && ev.key !== 'ArrowRight') return;
  const canSwitchWorkspaceTabs = Boolean(workspaceTabCatalogEl && !workspaceTabCatalogEl.hidden);
  if (isWorkspaceTab && !canSwitchWorkspaceTabs) return;
  ev.preventDefault();
  if (isWorkspaceTab) {
    const nextTab = activeWorkspaceTab === 'search' ? 'catalog' : 'search';
    setWorkspaceTab(nextTab, { focusTrigger: true });
    return;
  }
  const nextView = activeCatalogView === 'overview' ? 'browse' : 'overview';
  setCatalogView(nextView, { focusTrigger: true });
});

window.addEventListener('DOMContentLoaded', () => {
  currentLanguage = detectInitialLanguage();

  if (adminToolsDisclosureEl) {
    adminToolsDisclosureEl.open = false;
  }

  initDepartmentSelect();
  initCatalogSourceTypeSelect();
  initLanguageSelect();
  initDatePickers();
  initCursorBackgroundAura();
  setWorkspaceTab('search');
  setCatalogView('overview');
  setCatalogFiltersExpanded(false);
  applyTranslations();
  void loadSearchDepartments();
  void loadCatalogPage(1);

  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      document.body.classList.add('ui-mounted');
    });
  });
});
