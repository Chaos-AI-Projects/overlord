FROM nixos/nix:2.28.3

# Enable flakes and configure nix
RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

# Install dependencies via nix
RUN nix-channel --add https://nixos.org/channels/nixos-25.11 nixpkgs && \
    nix-channel --update

# Install system packages
RUN nix-env -iA \
    nixpkgs.python312 \
    nixpkgs.python312Packages.pip \
    nixpkgs.python312Packages.setuptools \
    nixpkgs.nodejs \
    nixpkgs.jq \
    nixpkgs.bash \
    nixpkgs.git \
    nixpkgs.curl \
    nixpkgs.shadow

# Install gws-cli (Google Workspace CLI) v0.22.5 with checksum verification
RUN curl -sL -o /tmp/gws.tar.gz https://github.com/googleworkspace/cli/releases/download/v0.22.5/google-workspace-cli-x86_64-unknown-linux-musl.tar.gz && \
    echo "4db473dde4b1ab872e4ff35d769b0d4af1f1a6441a605e79d5cf8ada9c87e920  /tmp/gws.tar.gz" | sha256sum -c - && \
    tar xzf /tmp/gws.tar.gz -C /usr/local/bin ./gws && \
    chmod +x /usr/local/bin/gws && \
    rm /tmp/gws.tar.gz

# Install Claude Code
RUN npm install -g @anthropic-ai/claude-code

# Copy source and install overlord
COPY . /app
WORKDIR /app

ENV LD_LIBRARY_PATH=/root/.nix-profile/lib

RUN pip install --break-system-packages -e .

# Create non-root overlord user
RUN useradd -m -s /bin/bash overlord

# Ensure nix-installed binaries and libraries are available to the overlord user
ENV PATH="/root/.nix-profile/bin:${PATH}"

# All persistent state lives under /home/overlord (single mounted volume)
VOLUME /home/overlord

# MCP HTTP server default port
EXPOSE 8000

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN chown -R overlord:overlord /app

# Entrypoint runs as root to detect volume UID and adjust overlord user,
# then drops privileges to run the daemon as overlord
ENTRYPOINT ["/entrypoint.sh"]
