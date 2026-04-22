use utoipa::OpenApi;

pub const SCALAR_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>Pulso API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      type="application/json"
      data-configuration='{
        "theme": "deepSpace",
        "layout": "modern",
        "showSidebar": true,
        "defaultOpenFirstTag": true,
        "hideModels": false,
        "withDefaultFonts": true,
        "operationTitleSource": "summary",
        "hideClientButton": false,
        "isEditable": false
      }'
    >$spec</script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>"#;

#[derive(OpenApi)]
#[openapi(
    info(title = "Pulso API", version = "0.1.0"),
    paths(
        // Health
        crate::routes::invoices::health,
        // Auth
        crate::routes::auth::register,
        crate::routes::auth::login,
        // Queue
        crate::routes::queue::list_jobs,
        crate::routes::queue::get_job,
        crate::routes::queue::get_job_results,
        crate::routes::queue::cancel_job,
        // Analytics
        crate::routes::analytics::get_summary,
        crate::routes::analytics::get_counterparties,
        crate::routes::analytics::get_recurrence,
        crate::routes::analytics::get_retention,
        crate::routes::analytics::get_geography,
        crate::routes::analytics::get_concepts,
        crate::routes::analytics::get_fiscal,
        crate::routes::analytics::get_payments,
        crate::routes::analytics::get_cashflow,
        crate::routes::analytics::get_payroll,
        // Normalization
        crate::routes::analytics::list_normalization,
        crate::routes::analytics::create_normalization,
        crate::routes::analytics::delete_normalization,
        crate::routes::analytics::list_payroll_normalization,
        crate::routes::analytics::create_payroll_normalization,
        crate::routes::analytics::delete_payroll_normalization,
        // Invoices
        crate::routes::invoices::list_invoices,
        crate::routes::invoices::download_invoices,
        crate::routes::invoices::xml_content,
    ),
    components(schemas(
        crate::routes::auth::RegisterDto,
        crate::routes::auth::LoginDto,
        crate::services::analytics::normalization::CreateRuleRequest,
        crate::services::analytics::normalization::CreatePayrollRuleRequest,
    )),
    tags(
        (name = "Health",        description = "Estado del servicio"),
        (name = "Auth",          description = "Registro e inicio de sesión vía Adquiere API"),
        (name = "Queue",         description = "Jobs de descarga SAT"),
        (name = "Analytics",     description = "Analítica de CFDIs"),
        (name = "Normalization", description = "Reglas de normalización de contrapartes y nómina"),
        (name = "Invoices",      description = "Descarga y consulta de CFDIs del SAT"),
    )
)]
pub struct ApiDoc;
