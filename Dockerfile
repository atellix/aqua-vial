from node:16-slim

RUN apt-get update && apt-get install -y git
RUN mkdir -p /usr/local/aqua-vial/idl
RUN mkdir -p /usr/local/aqua-vial/src
RUN mkdir -p /usr/local/aqua-vial/bin
COPY idl/ /usr/local/aqua-vial/idl
COPY src/ /usr/local/aqua-vial/src
COPY bin/ /usr/local/aqua-vial/bin
COPY package.json /usr/local/aqua-vial/package.json
COPY package-lock.json /usr/local/aqua-vial/package-lock.json
COPY tsconfig.json /usr/local/aqua-vial/tsconfig.json
WORKDIR /usr/local/aqua-vial
RUN npm install
COPY markets.json /usr/local/aqua-vial/markets.json
CMD node bin/aqua-vial.js --log-level=debug --markets-json=markets.json

