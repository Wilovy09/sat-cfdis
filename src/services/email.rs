use serde_json::json;

/// Human-friendly explanation of why a sync stalled, shown in the failure email.
fn failure_reason_es(error_code: Option<&str>) -> &'static str {
    match error_code {
        Some("invalid_credentials") => "la contraseña CIEC no fue aceptada por el SAT",
        Some("login_not_registered") => {
            "no pudimos confirmar el acceso al portal del SAT tras varios intentos automáticos"
        }
        Some("fiel_login_failed") => "no pudimos iniciar sesión con tu e.firma",
        Some("captcha_failed") => "el SAT no nos dejó pasar el captcha tras varios intentos",
        Some("sat_connection_error") | None => "el SAT no respondió tras varios intentos automáticos",
        _ => "tuvimos un problema técnico al conectar con el SAT",
    }
}

/// Sent when a sync job becomes permanently inaccessible — exhausted the
/// automatic retry backoff, or hit a non-retryable auth error — so the
/// client knows to check their credentials instead of wondering why their
/// dashboard is stuck.
pub async fn send_sync_failed(
    api_key: &str,
    from_email: &str,
    to_email: &str,
    rfc: &str,
    error_code: Option<&str>,
) -> anyhow::Result<()> {
    let reason = failure_reason_es(error_code);

    let plain_text = format!(
        "No pudimos completar la descarga de tus facturas del RFC {rfc}: {reason}. \
        Ya lo intentamos varias veces de forma automática. Entra a Pulso y revisa tus credenciales \
        (CIEC o e.firma) en tu perfil, o vuelve a intentar la sincronización. \
        Cualquier duda, escríbenos a soporte@adquiere.co y con gusto te ayudamos."
    );

    let html_body = format!(
        r#"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>No pudimos sincronizar tus facturas — Pulso</title>
</head>
<body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;">
          <tr>
            <td style="background:#00004e;padding:32px 40px;">
              <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;letter-spacing:-0.5px;">Pulso</h1>
            </td>
          </tr>
          <tr>
            <td style="padding:40px;">
              <h2 style="margin:0 0 16px;color:#b91c1c;font-size:20px;">No pudimos sincronizar tus facturas</h2>
              <p style="margin:0 0 16px;color:#374151;font-size:16px;line-height:1.6;">
                Intentamos varias veces descargar tus facturas del RFC <strong>{rfc}</strong>, pero
                {reason}.
              </p>
              <p style="margin:0 0 16px;color:#374151;font-size:16px;line-height:1.6;">
                Revisa tus credenciales (CIEC o e.firma) en tu perfil de Pulso, o vuelve a intentar la sincronización.
              </p>
              <p style="margin:0 0 32px;color:#374151;font-size:16px;line-height:1.6;">
                ¿Dudas? Escríbenos a
                <a href="mailto:soporte@adquiere.co" style="color:#00004e;font-weight:700;">soporte@adquiere.co</a>
                y con gusto te ayudamos.
              </p>
              <a href="https://pulso.adquiere.co/perfil"
                 style="display:inline-block;background:#00004e;color:#ffffff;text-decoration:none;
                        padding:14px 28px;border-radius:6px;font-size:16px;font-weight:600;">
                Revisar mi perfil
              </a>
            </td>
          </tr>
          <tr>
            <td style="padding:24px 40px;border-top:1px solid #e5e7eb;">
              <p style="margin:0;color:#9ca3af;font-size:13px;">
                Pulso · Adquiere &mdash; Este correo fue enviado automáticamente. Si necesitas ayuda,
                escríbenos a soporte@adquiere.co.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"#,
        rfc = rfc,
        reason = reason,
    );

    let subject = format!("No pudimos sincronizar tus facturas — RFC {rfc} · Pulso");

    let body = json!({
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email, "name": "Pulso"},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_text},
            {"type": "text/html",  "value": html_body}
        ]
    });

    let client = reqwest::Client::new();
    let response = client
        .post("https://api.sendgrid.com/v3/mail/send")
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("SendGrid returned {status}: {text}");
    }

    Ok(())
}

/// Notify a user that they've been added as a viewer to an RFC.
pub async fn send_rfc_invite(
    api_key: &str,
    from_email: &str,
    to_email: &str,
    rfc: &str,
    owner_email: &str,
) -> anyhow::Result<()> {
    let subject = format!("Te han agregado como invitado al RFC {rfc} en Pulso");

    let plain_text = format!(
        "{owner_email} te ha agregado como invitado al RFC {rfc} en Pulso. \
        Entra a pulso.adquiere.co para ver el análisis financiero de este RFC."
    );

    let html_body = format!(
        r#"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Invitación a RFC — Pulso</title>
</head>
<body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;">
          <tr>
            <td style="background:#00004e;padding:32px 40px;">
              <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;letter-spacing:-0.5px;">Pulso</h1>
            </td>
          </tr>
          <tr>
            <td style="padding:40px;">
              <h2 style="margin:0 0 16px;color:#00004e;font-size:20px;">Te han agregado como invitado</h2>
              <p style="margin:0 0 16px;color:#374151;font-size:16px;line-height:1.6;">
                <strong>{owner_email}</strong> te ha agregado como invitado al RFC
                <strong>{rfc}</strong> en Pulso.
              </p>
              <p style="margin:0 0 32px;color:#374151;font-size:16px;line-height:1.6;">
                Ahora puedes ver el análisis financiero de este RFC desde tu cuenta.
              </p>
              <a href="https://pulso.adquiere.co"
                 style="display:inline-block;background:#00004e;color:#ffffff;text-decoration:none;
                        padding:14px 28px;border-radius:6px;font-size:16px;font-weight:600;">
                Ver análisis
              </a>
            </td>
          </tr>
          <tr>
            <td style="padding:24px 40px;border-top:1px solid #e5e7eb;">
              <p style="margin:0;color:#9ca3af;font-size:13px;">
                Pulso · Adquiere &mdash; Este correo fue enviado automáticamente, no es necesario responderlo.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"#,
        owner_email = owner_email,
        rfc = rfc,
    );

    let body = json!({
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email, "name": "Pulso"},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_text},
            {"type": "text/html",  "value": html_body}
        ]
    });

    let client = reqwest::Client::new();
    let response = client
        .post("https://api.sendgrid.com/v3/mail/send")
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("SendGrid returned {status}: {text}");
    }

    Ok(())
}

/// Send a "monthly sync complete" notification — fired when the last day of a month finishes.
pub async fn send_monthly_complete(
    api_key: &str,
    from_email: &str,
    to_email: &str,
    rfc: &str,
    month_label: &str, // e.g. "Julio 2026"
) -> anyhow::Result<()> {
    let subject = format!("Facturas de {month_label} listas — RFC {rfc} · Pulso");

    let plain_text = format!(
        "Las facturas de {month_label} del RFC {rfc} ya están disponibles en Pulso. \
        Entra a pulso.adquiere.co para ver tu análisis actualizado."
    );

    let html_body = format!(
        r#"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Facturas de {month_label} listas — Pulso</title>
</head>
<body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;">
          <tr>
            <td style="background:#00004e;padding:32px 40px;">
              <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;letter-spacing:-0.5px;">Pulso</h1>
            </td>
          </tr>
          <tr>
            <td style="padding:40px;">
              <h2 style="margin:0 0 16px;color:#00004e;font-size:20px;">Facturas de {month_label} disponibles</h2>
              <p style="margin:0 0 16px;color:#374151;font-size:16px;line-height:1.6;">
                Las facturas de <strong>{month_label}</strong> del RFC <strong>{rfc}</strong>
                ya están sincronizadas en Pulso.
              </p>
              <p style="margin:0 0 32px;color:#374151;font-size:16px;line-height:1.6;">
                Entra a tu cuenta para ver el análisis financiero actualizado.
              </p>
              <a href="https://pulso.adquiere.co"
                 style="display:inline-block;background:#00004e;color:#ffffff;text-decoration:none;
                        padding:14px 28px;border-radius:6px;font-size:16px;font-weight:600;">
                Ver análisis
              </a>
            </td>
          </tr>
          <tr>
            <td style="padding:24px 40px;border-top:1px solid #e5e7eb;">
              <p style="margin:0;color:#9ca3af;font-size:13px;">
                Pulso · Adquiere &mdash; Este correo fue enviado automáticamente, no es necesario responderlo.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"#,
        month_label = month_label,
        rfc = rfc,
    );

    let body = serde_json::json!({
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email, "name": "Pulso"},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_text},
            {"type": "text/html",  "value": html_body}
        ]
    });

    let client = reqwest::Client::new();
    let response = client
        .post("https://api.sendgrid.com/v3/mail/send")
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("SendGrid returned {status}: {text}");
    }

    Ok(())
}

/// Send a "sync complete" notification to the user via SendGrid v3 Mail Send API.
pub async fn send_sync_complete(
    api_key: &str,
    from_email: &str,
    to_email: &str,
    rfc: &str,
    found: i64,
    period_from: &str,
    period_to: &str,
) -> anyhow::Result<()> {
    let period_label = format!(
        "{} → {}",
        &period_from[..7.min(period_from.len())],
        &period_to[..7.min(period_to.len())]
    );

    let plain_text = format!(
        "¡Tus facturas ya están listas! Descargamos {found} comprobantes del RFC {rfc} \
        correspondientes al período {period_label}. \
        Entra a Pulso para ver tu análisis financiero."
    );

    let html_body = format!(
        r#"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Descarga completada — Pulso</title>
</head>
<body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:40px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;">
          <!-- Header -->
          <tr>
            <td style="background:#00004e;padding:32px 40px;">
              <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;letter-spacing:-0.5px;">Pulso</h1>
            </td>
          </tr>
          <!-- Body -->
          <tr>
            <td style="padding:40px;">
              <h2 style="margin:0 0 16px;color:#00004e;font-size:20px;">Tu descarga del SAT ha terminado</h2>
              <p style="margin:0 0 16px;color:#374151;font-size:16px;line-height:1.6;">
                ¡Tus facturas ya están listas! Descargamos
                <strong>{found}</strong> comprobante{plural} del RFC <strong>{rfc}</strong>
                correspondientes al período <strong>{period_label}</strong>.
              </p>
              <p style="margin:0 0 32px;color:#374151;font-size:16px;line-height:1.6;">
                Entra a Pulso para ver tu análisis financiero actualizado.
              </p>
              <a href="https://pulso.adquiere.co"
                 style="display:inline-block;background:#00004e;color:#ffffff;text-decoration:none;
                        padding:14px 28px;border-radius:6px;font-size:16px;font-weight:600;">
                Ver mi análisis
              </a>
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="padding:24px 40px;border-top:1px solid #e5e7eb;">
              <p style="margin:0;color:#9ca3af;font-size:13px;">
                Pulso · Adquiere &mdash; Este correo fue enviado automáticamente, no es necesario responderlo.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"#,
        found = found,
        rfc = rfc,
        plural = if found == 1 { "" } else { "s" },
    );

    let subject = format!("Tu descarga del SAT ha terminado — RFC {rfc} · {period_label}");

    let body = json!({
        "personalizations": [
            {
                "to": [{"email": to_email}]
            }
        ],
        "from": {
            "email": from_email,
            "name": "Pulso"
        },
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain_text},
            {"type": "text/html",  "value": html_body}
        ]
    });

    let client = reqwest::Client::new();
    let response = client
        .post("https://api.sendgrid.com/v3/mail/send")
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("SendGrid returned {status}: {text}");
    }

    Ok(())
}
