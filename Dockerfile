# --------------------------------------------------------------------
# Base stage for getting all the dependencies installed
# --------------------------------------------------------------------
FROM python:3.8-alpine as install-deps
COPY . /code
WORKDIR /code
RUN script/cleanup \
	# these are needed for the `cryptography` pip package
	# which is used by `poetry`
	&& apk add libressl-dev musl-dev libffi-dev \
	# --
	&& apk add build-base \
	&& export PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
	&& pip install poetry \
	&& poetry config virtualenvs.create false \
	&& poetry install \
	&& rm -rf .git \
	&& apk del --purge build-base;


# ----------------------------------------
# Build stage specifically for development
# ----------------------------------------
# In case we want to be able to add new poetry dependencies
# through our container we reinstall `build-base` and instruct
# poetry to *not* use a virtualenv
FROM python:3.8-alpine as dev
COPY --from=install-deps /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages
COPY --from=install-deps /usr/local/bin /usr/local/bin
COPY --from=install-deps /root/.config/pypoetry /root/.config/pypoetry
ENV PYTHONASYNCIODEBUG=1
RUN apk add build-base \
	&& rm -rf /code;
WORKDIR /code
