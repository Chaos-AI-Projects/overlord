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
    nixpkgs.curl

# Install Claude Code
RUN npm install -g @anthropic-ai/claude-code

# Copy source and install overlord
COPY . /app
WORKDIR /app

ENV LD_LIBRARY_PATH=/root/.nix-profile/lib

RUN pip install --break-system-packages -e .

# Database lives on a runtime-attached volume
ENV XDG_DATA_HOME=/data
VOLUME /data
VOLUME /vault

# MCP HTTP server default port
EXPOSE 8000

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
