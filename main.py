from app import app
if __name__ == '__main__':
    app.secret_key = 'dhjfgjsdhfgdskjfhkjdshfdeslfjisdnhfiudshfinosdfcsndfcoi'
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run(debug=True)

