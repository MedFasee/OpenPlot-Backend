CREATE SCHEMA IF NOT EXISTS openplot;

CREATE TABLE IF NOT EXISTS openplot.sso_request_nonce (
	id UUID PRIMARY KEY,
	client_id VARCHAR(100) NOT NULL,
	nonce VARCHAR(200) NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	expires_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sso_request_nonce_client_nonce
	ON openplot.sso_request_nonce (client_id, nonce);

CREATE TABLE IF NOT EXISTS openplot.sso_login_token (
	id UUID PRIMARY KEY,
	token VARCHAR(500) NOT NULL,
	consulta_id VARCHAR(100) NOT NULL,
	origin_client VARCHAR(100) NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	expires_at TIMESTAMPTZ NOT NULL,
	used BOOLEAN NOT NULL DEFAULT FALSE,
	used_at TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sso_login_token_token
	ON openplot.sso_login_token (token);
