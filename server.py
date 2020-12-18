import connexion

options = {"swagger_ui": True}
app = connexion.App(__name__, specification_dir='openapi/', options=options)
app.add_api('bc.yml')
app.run(port=8080)