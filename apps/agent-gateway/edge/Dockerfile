# The demo's single public entry point. See edge/default.conf for why it exists.
#
# nginx-unprivileged rather than the stock nginx image: it already runs as a
# non-root UID and already listens on 8080, so there is nothing to patch and no
# entrypoint that needs to chown its way out of a read-only start. The stock
# image starts as root and drops privileges itself, which works but means
# `USER nginx` quietly breaks its /docker-entrypoint.d scripts.
FROM nginxinc/nginx-unprivileged:1.29-alpine

# Replace the stock server block rather than adding beside it: the stock one is
# also a catch-all, and two catch-alls on one host is the kind of ambiguity that
# surfaces as an intermittent 404 rather than a startup error.
COPY edge/default.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080
