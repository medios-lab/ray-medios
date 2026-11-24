# Image for every Ray node of the XGBoost Cover type demo: Ray and the
# workload's Python packages, so that a task installs nothing when it starts.
#
# Base: CPython 3.12 on Debian (pinned by digest).
FROM python@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df

# requirements.txt is exported from uv.lock with the kernel group, so the
# image can also run the notebook's Jupyter server and kernel in a task. Every package carries hashes; pip refuses a download
# that does not match.
COPY requirements.txt /opt/ray-venv-requirements.txt
RUN set -eu; \
    python3 -m venv /opt/ray-venv; \
    /opt/ray-venv/bin/pip install --no-cache-dir -q --require-hashes -r /opt/ray-venv-requirements.txt; \
    /opt/ray-venv/bin/pip freeze > /opt/ray-venv/pip-freeze.txt

ENV PATH=/opt/ray-venv/bin:$PATH

# The framework's tasks need ray, sh, base64 and mktemp at start; the
# workload needs the imports.
RUN set -eu; ray --version; for c in sh base64 mktemp; do command -v "$c"; done; \
    python3 -c 'import ray.train.xgboost, ray.data, xgboost, sklearn, pyarrow, pandas, matplotlib, seaborn, ipykernel, nbconvert, ipywidgets'; \
    jupyter lab --version

# No entrypoint: docker_shell keeps an image's ENTRYPOINT and runs the task
# command as its arguments.
ENTRYPOINT []
CMD ["sh"]
