<?php

declare(strict_types=1);

namespace Adquiere\CfdiCli;

use GuzzleHttp\Exception\RequestException;
use PhpCfdi\CfdiSatScraper\Exceptions\SatHttpGatewayClientException;
use PhpCfdi\CfdiSatScraper\Exceptions\SatHttpGatewayException;
use PhpCfdi\CfdiSatScraper\Exceptions\SatHttpGatewayResponseException;
use PhpCfdi\CfdiSatScraper\Sessions\Ciec\CiecLoginException;
use PhpCfdi\CfdiSatScraper\Sessions\Fiel\FielLoginException;
use Throwable;

/**
 * Classifies a caught Throwable from the SAT scraper into a stable code + a
 * user-facing message + how long (seconds) to wait before retrying, so the
 * Rust layer never has to guess from raw exception text.
 *
 * IMPORTANT: never fall back to "instanceof SatException" — every exception
 * this library throws implements that interface (wrong password, unsolvable
 * captcha, invalid FIEL cert, and a plain transient SAT 500 alike), so a
 * blanket check there silently misclassifies all of them as a rate limit.
 */
final class ErrorClassifier
{
    /** Codes where retrying with the same input reproduces the same failure. */
    private const AUTH_ERROR_CODES = [
        'invalid_credentials',
        'captcha_failed',
        'login_not_registered',
        'fiel_login_failed',
        'unknown_error',
    ];

    /** @return array{code: string, message: string, retry_after: int} */
    public static function classify(Throwable $e): array
    {
        $chain = [$e];
        $cursor = $e;
        while (($cursor = $cursor->getPrevious()) !== null) {
            $chain[] = $cursor;
        }

        foreach ($chain as $link) {
            $msg = $link->getMessage();

            if ($link instanceof CiecLoginException) {
                if ($msg === 'Incorrect login data') {
                    return ['code' => 'invalid_credentials', 'message' => $msg, 'retry_after' => 0];
                }
                if (str_contains($msg, 'captcha')) {
                    return ['code' => 'captcha_failed', 'message' => $msg, 'retry_after' => 0];
                }
                if (str_starts_with($msg, 'It was expected to have the session registered')) {
                    return ['code' => 'login_not_registered', 'message' => $msg, 'retry_after' => 0];
                }
                if (str_starts_with($msg, 'Connection error when')) {
                    // $link wraps the real SatHttpGatewayException as $previous —
                    // inspect ITS actual HTTP status, not just this template message.
                    return self::classifyHttpFailure($link->getPrevious() ?? $link, $msg);
                }
            }

            if ($link instanceof FielLoginException) {
                if (str_starts_with($msg, 'It was expected to have the session registered')) {
                    return ['code' => 'login_not_registered', 'message' => $msg, 'retry_after' => 0];
                }
                if (str_starts_with($msg, 'Connection error when')) {
                    return self::classifyHttpFailure($link->getPrevious() ?? $link, $msg);
                }
                return ['code' => 'fiel_login_failed', 'message' => $msg, 'retry_after' => 0];
            }

            if ($link instanceof SatHttpGatewayException) {
                return self::classifyHttpFailure($link, $msg);
            }

            // Explicit, narrow rate-limit signals only — genuine SAT-imposed throttling.
            if (
                str_contains($msg, '429')
                || str_contains($msg, 'Too Many')
                || str_contains($msg, 'bloqueado')
                || str_contains($msg, 'demasiadas')
                || str_contains($msg, 'limite')
                || str_contains($msg, 'limit')
                || str_contains($msg, 'No tiene acceso')
                || str_contains($msg, 'Folio: DT-')
                || str_contains($msg, 'falla en el servicio')
                || str_contains($msg, 'Se ha presentado una falla')
            ) {
                return ['code' => 'rate_limited', 'message' => $msg, 'retry_after' => 24 * 3600 + 1800];
            }
        }

        return ['code' => 'unknown_error', 'message' => $e->getMessage(), 'retry_after' => 0];
    }

    public static function isAuthErrorCode(string $code): bool
    {
        return in_array($code, self::AUTH_ERROR_CODES, true);
    }

    /**
     * Real HTTP status code behind a SatHttpGatewayException, when one is
     * available. Null for pure connection failures (DNS, refused, timeout —
     * Guzzle's ConnectException never carries a response).
     */
    private static function httpStatusCode(Throwable $e): ?int
    {
        if ($e instanceof SatHttpGatewayResponseException) {
            return $e->getResponse()->getStatusCode();
        }
        if ($e instanceof SatHttpGatewayClientException) {
            $inner = $e->getClientException();
            if ($inner instanceof RequestException && $inner->hasResponse()) {
                return $inner->getResponse()?->getStatusCode();
            }
        }
        return null;
    }

    /**
     * 429/503 are SAT explicitly throttling us — worth the same long backoff
     * as a real daily-quota hit. Everything else (500, other 5xx, a plain
     * timeout/connection-refused with no status at all) is treated as a
     * transient glitch worth retrying soon.
     *
     * @return array{code: string, message: string, retry_after: int}
     */
    private static function classifyHttpFailure(Throwable $httpException, string $message): array
    {
        $status = self::httpStatusCode($httpException);
        if ($status === 429 || $status === 503) {
            return ['code' => 'rate_limited', 'message' => $message, 'retry_after' => 24 * 3600 + 1800];
        }
        return ['code' => 'sat_connection_error', 'message' => $message, 'retry_after' => 900];
    }
}
