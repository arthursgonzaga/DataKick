# Minio

Ferramenta de abstração para storage que permite gerenciar arquivos de forma simples e eficiente. Ele será, no nosso projeto, uma alternativa ao S3 da AWS. Aqui usaremos o docker para executar o processo e para configuração realizamos as seguintes etapas:

1. Com o docker em execução, realizar o pull da imagem do minio: `docker pull minio/minio`
2. Realizar a criação do arquivo de configuração `.env` (aqui criado na pasta raíz) com o seguinte conteúdo:

    ```bash
        MINIO_ROOT_USER=your_user
        MINIO_ROOT_PASSWORD=your_password
    ```

    Substituir `MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD` pelos valores desejados.

3. Rodar esse comando:

    ```bash
        docker run -d \
        -p 9000:9000 \
        -p 9001:9001 \
        -v ./minio/data:/data \
        -v ./.env:/etc/config.env \
        -e "MINIO_CONFIG_ENV_FILE=/etc/config.env" \
        --name "minio" \
        quay.io/minio/minio server /data --console-address :9001
    ```

4. Acessar o endereço `http://localhost:9001` e realizar o login com as credenciais definidas no arquivo `.env`.

5. Criar um bucket com o nome `datakick-raw` e pronto, o minio está configurado e pronto para uso.

6. É possível gerar credenciais de acesso para o bucket criado, para isso basta acessar a aba `Access Keys` e criar um novo usuário.
