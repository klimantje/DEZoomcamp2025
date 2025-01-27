FROM python:3.9

COPY requirements.txt .
COPY week_1/pipeline.py pipeline.py
RUN pip install -r requirements.txt

# entrypoint is not for running with devcontainer.
# ENTRYPOINT ["python", "pipeline.py"]


#Install terraform

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
  && rm -rf /var/lib/apt/lists/*

RUN wget --quiet https://releases.hashicorp.com/terraform/1.6.6/terraform_1.6.6_linux_amd64.zip \
  && unzip terraform_1.6.6_linux_amd64.zip \
  && mv terraform /usr/bin \
  && rm terraform_1.6.6_linux_amd64.zip

# Install cloud sdk
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-327.0.0-linux-x86_64.tar.gz \
    -O /tmp/google-cloud-sdk.tar.gz | bash

RUN mkdir -p /usr/local/gcloud \
    && tar -C /usr/local/gcloud -xvzf /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh -q

ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin