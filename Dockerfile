FROM python:3.7-slim

# TODO: Security concerns about running in root (KIV for now)
# ARG USER_NAME=app_user
# ARG GROUP_NAME=app_user
# ARG OS_UID=1200
# ARG OS_GID=1200

# ENV PIP_ROOT_USER_ACTION=ignore

# Create the user
# RUN groupadd --gid $OS_GID $GROUP_NAME \
# && useradd --uid $OS_UID --gid $OS_GID -m $USER_NAME \
# && /usr/local/bin/python -m pip install --upgrade  pip
   
# [Optional] Set the default user. Omit if you want to keep the default as root.
# USER $USER_NAME

# # Update path (because we are in user)
# ENV PATH="$PATH:/home/$USER_NAME/.local/bin"

# START

WORKDIR /app

COPY ./app .

RUN /usr/local/bin/python -m pip install --upgrade  pip && pip install -r requirements.txt

ENTRYPOINT [ "python" ] 
CMD [ "main.py", "/mnt/secrets/amqp/.cloudampq.json", "/mnt/secrets/database/.database-config.json", "/usr/share/generic-128mi/sgx/json" ]