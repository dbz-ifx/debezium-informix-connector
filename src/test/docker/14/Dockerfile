FROM ibmcom/informix-developer-database:14.10.FC7W1DE

ADD --chown=informix:informix informix_post_init.sh /opt/ibm/config

ENV SIZE small
ENV LICENSE accept
ENV RUN_FILE_POST_INIT informix_post_init.sh
