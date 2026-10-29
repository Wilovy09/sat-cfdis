<?php

/**
 * Standalone assertions for Adquiere\CfdiCli\ErrorClassifier — no PHPUnit
 * dependency, run directly with `php php-cli/tests/ErrorClassifierTest.php`.
 * Exits 0 and prints "OK" when every case passes, else prints the failures
 * and exits 1.
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use Adquiere\CfdiCli\ErrorClassifier;
use PhpCfdi\CfdiSatScraper\Exceptions\SatHttpGatewayClientException;
use PhpCfdi\CfdiSatScraper\Exceptions\SatHttpGatewayResponseException;
use PhpCfdi\CfdiSatScraper\Sessions\Ciec\CiecLoginException;
use PhpCfdi\CfdiSatScraper\Sessions\Ciec\CiecSessionData;
use PhpCfdi\CfdiSatScraper\Sessions\Fiel\FielLoginException;
use PhpCfdi\CfdiSatScraper\Sessions\Fiel\FielSessionData;
use PhpCfdi\Credentials\Credential;
use PhpCfdi\ImageCaptchaResolver\CaptchaAnswerInterface;
use PhpCfdi\ImageCaptchaResolver\CaptchaImageInterface;
use PhpCfdi\ImageCaptchaResolver\CaptchaResolverInterface;

// CiecSessionData requires a resolver instance but none of these fixtures
// ever call resolve() — a stub that always throws is enough.
$stubCaptchaResolver = new class implements CaptchaResolverInterface {
    public function resolve(CaptchaImageInterface $image): CaptchaAnswerInterface
    {
        throw new RuntimeException('not used by this test');
    }
};

$fixtureDir = __DIR__ . '/../../libs/cfdi-sat-scraper/tests/_files/fake-fiel';
$fielCredential = Credential::openFiles(
    "$fixtureDir/EKU9003173C9.cer",
    "$fixtureDir/EKU9003173C9.key",
    trim(file_get_contents("$fixtureDir/EKU9003173C9.pwd")),
);
$fielData = new FielSessionData($fielCredential);

$failures = [];
$assertionCount = 0;

function check(array &$failures, string $name, string $expectedCode, array $actual): void
{
    global $assertionCount;
    $assertionCount++;
    if ($actual['code'] !== $expectedCode) {
        $failures[] = "$name: expected code '$expectedCode', got '{$actual['code']}'";
    }
}

$ciecData = new CiecSessionData('AAA010101AAA', 'clave', $stubCaptchaResolver);

// -- invalid_credentials: wrong CIEC password ------------------------------
check(
    $failures,
    'wrong CIEC password',
    'invalid_credentials',
    ErrorClassifier::classify(CiecLoginException::incorrectLoginData($ciecData, '<html>Ecom_User_ID</html>', [])),
);

// -- captcha_failed ---------------------------------------------------------
check(
    $failures,
    'captcha image not found',
    'captcha_failed',
    ErrorClassifier::classify(CiecLoginException::noCaptchaImageFound($ciecData, '<html></html>')),
);

// -- login_not_registered ----------------------------------------------------
check(
    $failures,
    'session not registered after login',
    'login_not_registered',
    ErrorClassifier::classify(CiecLoginException::notRegisteredAfterLogin($ciecData, '<html></html>')),
);

// -- sat_connection_error: THE bug from the captured SAT 500 -----------------
// A transient SAT-side outage must resolve to sat_connection_error, never to
// invalid_credentials (old Rust stderr substring bug) nor rate_limited (old
// PHP "instanceof SatException" catch-all bug).
$responseException = SatHttpGatewayResponseException::unexpectedEmptyResponse(
    'sending login data',
    new GuzzleHttp\Psr7\Response(500),
    'POST',
    'https://login.siat.sat.gob.mx/nidp/app/login',
    [],
);
$connectionWrapped = CiecLoginException::connectionException('sending login data', $ciecData, $responseException);
check($failures, 'wrapped SAT 500 during login (CIEC)', 'sat_connection_error', ErrorClassifier::classify($connectionWrapped));
check($failures, 'raw SatHttpGatewayResponseException', 'sat_connection_error', ErrorClassifier::classify($responseException));

$clientException = SatHttpGatewayClientException::clientException(
    'sending login data',
    'POST',
    'https://login.siat.sat.gob.mx/nidp/app/login',
    [],
    [],
    new GuzzleHttp\Exception\ConnectException('timed out', new GuzzleHttp\Psr7\Request('POST', 'https://login.siat.sat.gob.mx')),
);
check($failures, 'raw SatHttpGatewayClientException (timeout, no status)', 'sat_connection_error', ErrorClassifier::classify($clientException));

// -- rate_limited via REAL HTTP status code (429/503), not message sniffing --
// A genuine throttle response deserves the same 24.5h backoff as the daily
// quota, not the 15min "just a glitch" retry.
$request = new GuzzleHttp\Psr7\Request('POST', 'https://login.siat.sat.gob.mx/nidp/app/login');

$response429 = SatHttpGatewayResponseException::unexpectedEmptyResponse(
    'sending login data',
    new GuzzleHttp\Psr7\Response(429),
    'POST',
    'https://login.siat.sat.gob.mx/nidp/app/login',
    [],
);
check($failures, 'SatHttpGatewayResponseException with real 429', 'rate_limited', ErrorClassifier::classify($response429));

$clientException503 = SatHttpGatewayClientException::clientException(
    'sending login data',
    'POST',
    'https://login.siat.sat.gob.mx/nidp/app/login',
    [],
    [],
    new GuzzleHttp\Exception\ServerException('503 Service Unavailable', $request, new GuzzleHttp\Psr7\Response(503)),
);
check($failures, 'SatHttpGatewayClientException wrapping a real 503', 'rate_limited', ErrorClassifier::classify($clientException503));

// A 500 (or any non-429/503 status) must stay sat_connection_error, not rate_limited.
$clientException500 = SatHttpGatewayClientException::clientException(
    'sending login data',
    'POST',
    'https://login.siat.sat.gob.mx/nidp/app/login',
    [],
    [],
    new GuzzleHttp\Exception\ServerException('500 Internal Server Error', $request, new GuzzleHttp\Psr7\Response(500)),
);
check($failures, 'SatHttpGatewayClientException wrapping a real 500', 'sat_connection_error', ErrorClassifier::classify($clientException500));

// The CIEC/FIEL "Connection error when ..." wrapper must also honor the
// real status code of what it wraps, not just default to sat_connection_error.
$ciecWrapping429 = CiecLoginException::connectionException('sending login data', $ciecData, $response429);
check($failures, 'CiecLoginException wrapping a real 429', 'rate_limited', ErrorClassifier::classify($ciecWrapping429));

// -- fiel_login_failed / FIEL connection & registration errors ---------------
check(
    $failures,
    'FIEL session not registered after login',
    'login_not_registered',
    ErrorClassifier::classify(FielLoginException::notRegisteredAfterLogin($fielData, '<html></html>')),
);
check(
    $failures,
    'FIEL connection error (transient SAT outage during e.firma login)',
    'sat_connection_error',
    ErrorClassifier::classify(FielLoginException::connectionException('sending login data', $fielData)),
);
check(
    $failures,
    'generic FielLoginException (e.g. invalid/expired cert)',
    'fiel_login_failed',
    ErrorClassifier::classify(new FielLoginException('e.firma certificate rejected', '', $fielData)),
);

// -- rate_limited: explicit SAT throttling signals only ----------------------
check(
    $failures,
    'explicit 429 signal',
    'rate_limited',
    ErrorClassifier::classify(new RuntimeException('HTTP 429 Too Many Requests')),
);
check(
    $failures,
    'explicit "bloqueado" signal',
    'rate_limited',
    ErrorClassifier::classify(new RuntimeException('El servicio se encuentra bloqueado temporalmente')),
);

// -- unknown_error: anything unrecognized must NOT silently become a rate limit
check(
    $failures,
    'unrecognized exception',
    'unknown_error',
    ErrorClassifier::classify(new RuntimeException('Something SAT has never said before')),
);

// -- isAuthErrorCode() ---------------------------------------------------------
$authCodes = ['invalid_credentials', 'captcha_failed', 'login_not_registered', 'fiel_login_failed', 'unknown_error'];
foreach ($authCodes as $code) {
    $assertionCount++;
    if (!ErrorClassifier::isAuthErrorCode($code)) {
        $failures[] = "isAuthErrorCode('$code') should be true";
    }
}
foreach (['sat_connection_error', 'rate_limited'] as $code) {
    $assertionCount++;
    if (ErrorClassifier::isAuthErrorCode($code)) {
        $failures[] = "isAuthErrorCode('$code') should be false (retryable via pause/resume, not a hard fail)";
    }
}

if ($failures) {
    fwrite(STDERR, "FAILED:\n  - " . implode("\n  - ", $failures) . "\n");
    exit(1);
}

echo "OK — $assertionCount assertions passed\n";
