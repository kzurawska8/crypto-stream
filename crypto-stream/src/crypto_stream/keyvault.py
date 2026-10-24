"""Resolve the Event Hubs / Kafka password, optionally from Azure Key Vault.

If both KEYVAULT_URL and KEYVAULT_SECRET_NAME are set, the connection string is
fetched from Azure Key Vault, authenticated with Entra ID via
`DefaultAzureCredential`. That credential tries several methods in order, so it
works both for:

* local dev   — run `az login` once, nothing else to configure; and
* a service principal (app identity) — set AZURE_TENANT_ID, AZURE_CLIENT_ID,
  AZURE_CLIENT_SECRET (the "real app" way; the secret lives only in `.env`).

If Key Vault isn't configured, the plain KAFKA_SASL_PASSWORD value is used — so
local Kafka and the simple paste-the-string setup keep working unchanged.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def load_kafka_password(config) -> str:
    # No Key Vault configured → use the password straight from the environment.
    if not (config.keyvault_url and config.keyvault_secret_name):
        return config.kafka_sasl_password

    # Lazy import: the azure SDKs are only needed on this Key Vault path.
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=config.keyvault_url, credential=credential)
    secret = client.get_secret(config.keyvault_secret_name)
    log.info(
        "loaded secret '%s' from Key Vault %s via Entra ID",
        config.keyvault_secret_name,
        config.keyvault_url,
    )
    return secret.value
