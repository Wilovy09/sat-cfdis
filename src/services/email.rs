use serde_json::json;

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
