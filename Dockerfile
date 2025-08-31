FROM python:slim-bullseye

RUN apt-get update -y && \
    apt-get install --no-install-recommends -y curl ca-certificates nano tzdata cron wget && \
    rm -rf /var/lib/apt/lists/* && \
    curl --fail --show-error --location --proto '=https' --tlsv1.2 \
         https://astral.sh/uv/install.sh | sh && \
    cp /root/.local/bin/uv /usr/local/bin/uv && \
    chmod a+rx /usr/local/bin/uv

# Set timezone
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/"$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

WORKDIR /app

COPY . .

RUN uv venv .venv && \
    uv sync

CMD ["uv", "run", "listing_strategy.py"]