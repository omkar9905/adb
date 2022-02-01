from pprint import pprint

from flask import Flask, render_template, request, redirect, url_for, flash
import os
from datetime import datetime, timedelta
import urllib
import pandas as pd
from os.path import join, dirname, realpath
import pymssql
from sqlalchemy import create_engine
from azure.storage.blob import BlobClient,generate_blob_sas, BlobSasPermissions,PublicAccess,BlobServiceClient
import urllib.request
#from PIL import Image

app = Flask(__name__)
UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] =  UPLOAD_FOLDER

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/uploadFile', methods=['GET', 'POST'])
def uploadFile():
    if request.method == "POST":
        uploaded_file = request.files['file']
        if uploaded_file.filename != '':
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
            # set the file path
            uploaded_file.save(file_path)
            parseCSV(file_path)
            flash('File Uploaded')
        # save the file
    return render_template("upload.html")

def parseCSV(filepath):
    if filepath.endswith('.csv'):
        server = 'oxg7237.database.windows.net'
        user = 'oxg7237'
        password = 'Omkargawade@96'
        conn = pymssql.connect(server, user, password, "temp")
        c1 = conn.cursor()
        col_names=['name','state','salary','grade','room','telnum','picture','keywords']
        csvData = pd.read_csv(filepath,names=col_names,header=None)
        csvData = csvData.where((pd.notnull(csvData)), None)
        for i, row in csvData.iterrows():
            if i > 0:
                try:
                    sql = "INSERT INTO users (name,state, salary,grade, room, telnum, picture, keywords) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)"
                    value = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
                    c1.execute(sql, value)
                    print(row['name'],row['state'], row['salary'],row['grade'], row['room'], row['telnum'], row['picture'], row['keywords'])
                    conn.commit()
                    conn.close

                except pymssql._pymssql.IntegrityError:
                    flash("Duplicate names in file")

    elif filepath.endswith(('.png','.jpg')):
        path = 'static/files'
        file_names = os.listdir(path)
        account_name = 'sqlva7zekxza2qevhw'
        account_key = 'A48+JUxoxbggHcwf1RPyBu/A7z5g3Fv6eB47rTnCWUwf+EcWL0on2q9PVh9vy/Inhc9fOfKg1YiyuXN3AeJheA=='
        container_name = 'test'
        connection_string = 'DefaultEndpointsProtocol=https;AccountName=sqlva7zekxza2qevhw;AccountKey=A48+JUxoxbggHcwf1RPyBu/A7z5g3Fv6eB47rTnCWUwf+EcWL0on2q9PVh9vy/Inhc9fOfKg1YiyuXN3AeJheA==;EndpointSuffix=core.windows.net'
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        for file_name in file_names:
            if file_name.endswith(('.png','.jpg')):
                blob_name = file_name
                file_path = path+'/'+file_name
                blob_client = container_client.get_blob_client(blob_name)
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
    else:
        pass

@app.route('/search', methods=['GET','POST'])
def search():
    account_name = 'sqlva7zekxza2qevhw'
    account_key = 'A48+JUxoxbggHcwf1RPyBu/A7z5g3Fv6eB47rTnCWUwf+EcWL0on2q9PVh9vy/Inhc9fOfKg1YiyuXN3AeJheA=='
    container_name = 'test'
    urls = None

    if request.method == "POST":
        user = request.form['user']
        salaryGrt = request.form['salaryGrt']
        salaryLes = request.form['salaryLes']
        if user:
            names = getPicture(user)
        elif salaryGrt:
            names = salaryGrtquery(salaryGrt)
        else:
            names = salaryLesquery(salaryLes)
        urls = []
        print(names)

        for name in names:
            blob_name = name
            blob = get_blob_sas(account_name, account_key, container_name, blob_name)
            url = 'https://' + account_name + '.blob.core.windows.net/' + container_name + '/' + blob_name + '?' + blob

            urls.append(url)
    return render_template('search.html',urls=urls)

def getPicture(name):
    server = 'oxg7237.database.windows.net'
    user = 'oxg7237'
    password = 'Omkargawade@96'
    conn = pymssql.connect(server, user, password, "temp")
    c1 = conn.cursor()

    try:
        sql = f"select picture from users where name='{name}'"
        c1.execute(sql)
        names = c1.fetchall()
        for i, name in enumerate(names):
            names[i] = name[0]
    except:
        flash("IMAGE FILE DOES NOT EXIST")
    return names


def salaryGrtquery(salaryGrt):
    server = 'oxg7237.database.windows.net'
    user = 'oxg7237'
    password = 'Omkargawade@96'
    conn = pymssql.connect(server, user, password, "temp")
    c1 = conn.cursor()

    try:
        sql = f"select picture from users where salary > {salaryGrt}"
        c1.execute(sql)
        names = c1.fetchall()
        for i,name in enumerate(names):
            names[i] = name[0]
    except:
        pass
    return names

def salaryLesquery(salaryLes):
    server = 'oxg7237.database.windows.net'
    user = 'oxg7237'
    password = 'Omkargawade@96'
    conn = pymssql.connect(server, user, password, "temp")
    c1 = conn.cursor()

    try:
        sql = f"select picture from users where salary < {salaryLes}"
        c1.execute(sql)
        names = c1.fetchall()
        for i,name in enumerate(names):
            names[i] = name[0]
    except:
        pass
    return names

def get_blob_sas(account_name, account_key, container_name, blob_name):
    sas_blob = generate_blob_sas(account_name=account_name,
                                 container_name=container_name,
                                 blob_name=blob_name,
                                 account_key=account_key,
                                 permission=BlobSasPermissions(read=True),
                                 expiry=datetime.utcnow() + timedelta(hours=2))
    return sas_blob

def getNames():
    server = 'oxg7237.database.windows.net'
    user = 'oxg7237'
    password = 'Omkargawade@96'
    conn = pymssql.connect(server, user, password, "temp")
    c1 = conn.cursor()
    names = []

    try:
        sql = "SELECT * from users"
        c1.execute(sql)
        names = c1.fetchall()
    except:
        pass
    return names

@app.route('/edit',methods=['GET','POST'])
def edit():
    server = 'oxg7237.database.windows.net'
    user = 'oxg7237'
    password = 'Omkargawade@96'
    conn = pymssql.connect(server, user, password, "temp")
    c1 = conn.cursor()
#Name,State,Salary,Grade,Room,Telnum,Picture,Keywords
    if request.method == "POST":
        name = request.form['name'].lower()
        state=request.form['state']
        salary = request.form['salary']
        grade=request.form['grade']
        room = request.form['room']
        telnum = request.form['telnum']
        picture = request.form['picture']
        keywords = request.form['keywords']
        print(picture)
        if salary:
            try:
                if picture:
                    print('HERE')
                    sql = f"UPDATE users set name = '{name}',state = '{state}', salary = {salary},grade = {grade}, room={room},telnum={telnum},picture='{picture}',keywords='{keywords}' where name = '{name}'"
                    print(sql)
                else:
                    sql = f"UPDATE users set name = '{name}',state = '{state}', salary = {salary},grade = {grade}, room={room},telnum={telnum},picture='{picture}',keywords='{keywords}' where name = '{name}'"
                    print(sql)
                c1.execute(sql)
                conn.commit()
                conn.close
            except pymssql._pymssql.IntegrityError:
                flash("Duplicate names in file")

    names = getNames()
    return render_template("edit.html", names=names)
