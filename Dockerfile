FROM python:3.13-rc-alpine3.18
WORKDIR /app
COPY . /app
RUN apk update
RUN apk add python3-dev libc-dev g++ libffi-dev libxml2 unixodbc-dev build-base gnupg
RUN apk --no-cache add curl sudo
RUN curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.10.5.1-1_amd64.apk \
    && curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/mssql-tools_17.10.1.1-1_amd64.apk \
    #(Optional) Verify signature, if 'gpg' is missing install it using 'apk add gnupg':
    && curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/msodbcsql17_17.10.5.1-1_amd64.sig \
    && curl -O https://download.microsoft.com/download/e/4/e/e4e67866-dffd-428c-aac7-8d28ddafb39b/mssql-tools_17.10.1.1-1_amd64.sig \
    && curl https://packages.microsoft.com/keys/microsoft.asc  | gpg --import - \
    && gpg --verify msodbcsql17_17.10.5.1-1_amd64.sig msodbcsql17_17.10.5.1-1_amd64.apk \
    && gpg --verify mssql-tools_17.10.1.1-1_amd64.sig mssql-tools_17.10.1.1-1_amd64.apk \
    #Install the package(s)
    && sudo apk add --allow-untrusted msodbcsql17_17.10.5.1-1_amd64.apk \
    && sudo apk add --allow-untrusted mssql-tools_17.10.1.1-1_amd64.apk
RUN pip install -r requirements.txt
EXPOSE 5000
CMD ["python", "app.py"]