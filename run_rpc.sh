#!/bin/bash
export MINION_HOST=173.234.24.74
#export KEY_FILE_NAME=privkey.pem
#export CERT_FILE_NAME=fullchain.pem
export POSTGRES_HOST=173.234.24.76
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_DATABASE=markets
export POSTGRES_PASSWORD=TZauF6C8fe0jgsD
export ANCHOR_PROVIDER_URL=https://api.devnet.solana.com
export ANCHOR_WALLET=/home/mfrager/atx/crank/wallet_1.json

node bin/aqua-vial.js --log-level=debug --markets-json=markets.json --tokens-json=tokens.json --service-mode=rpc --port=8001
