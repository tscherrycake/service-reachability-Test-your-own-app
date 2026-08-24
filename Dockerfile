FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends xvfb xauth \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install --with-deps chromium

COPY . .

EXPOSE 5000

# Xvfb gives Chromium a real (virtual) X display so it launches headed
# (HEADLESS = False in run_full_pipeline.py, for stealth) without ever
# rendering to a screen anyone can see. Started directly (not via xvfb-run,
# whose SIGUSR1 readiness handshake hangs in this environment).
#
# rm the stale lock/socket first: some hosts (Render included) restart the
# container's process after a crash without always giving it a fresh
# filesystem, so a previous Xvfb's /tmp/.X99-lock can survive and make the
# new Xvfb instance fail with "Server is already active for display 99" —
# which this script used to swallow silently and start Flask anyway with a
# DISPLAY pointing at nothing, breaking every subsequent browser launch.
CMD rm -f /tmp/.X99-lock /tmp/.X11-unix/X99; \
    Xvfb :99 -screen 0 1280x1024x24 -ac & \
    sleep 1 && \
    export DISPLAY=:99 && \
    exec python server.py
