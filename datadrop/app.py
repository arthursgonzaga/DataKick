import os
from flask import Flask, render_template, request, redirect, flash
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import boto3

load_dotenv()

app = Flask(__name__)
app.secret_key = 'datakick'  # Necessário para o Flask-WTF
app.config['UPLOAD_FOLDER'] = './uploads'

s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_URL"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

BUCKET_NAME = os.getenv("MANUAL_BUCKET_NAME")

try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    pass


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Obter o nome da tabela e o arquivo
        table_name = request.form.get('table_name')
        file = request.files.get('file')

        if not table_name or not file:
            flash('Por favor, preencha todos os campos.')
            return redirect(request.url)

        # Salvar o arquivo localmente
        filename = secure_filename(file.filename)
        local_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        file.save(local_path)

        # Enviar para o MinIO
        s3_key = f"datadrop/{table_name}/{filename}"
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)

        # Limpar arquivos locais
        os.remove(local_path)

        flash('Upload concluído e enviado para o S3!')
        return redirect(request.url)

    return render_template('upload.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False)