from flask import Flask , make_response ,request ,redirect ,url_for , jsonify , Blueprint
from werkzeug.serving import  WSGIRequestHandler
'''
HTTPS is a must

use mkcert

FLASK_ENV=development flask run --cert=./127.0.0.1.pem --key=./127.0.0.1-key.pem

gaia cannot have PATH ? otherwise X-Chrome-ID-Consistency-Request will not be injected? see signin_header_helper ..

'''
app = Flask(__name__)
'''
GOOGLE_API_KEY=1234 GOOGLE_DEFAULT_CLIENT_ID=1234 GOOGLE_DEFAULT_CLIENT_SECRET=2345 chromium-browser --enable-logging=stderr --v=1 --gaia-url=https://127.0.0.1:5000 --sync-url=https://127.0.0.1:5000/sync/ --oauth-account-manager-url=https://127.0.0.1:5000/oauth/ --google-apis-url=https://127.0.0.1:5000/apis/ --lso-url=https://127.0.0.1:5000/lso/ --google-url=https://127.0.0.1:5000/google/ --oauth2-client-id=1234 --oauth2-client-secret=2345
'''


## GAIA part
@app.route("/signin/chrome/sync")
def sync():
    print(request.headers.get("X-Chrome-ID-Consistency-Request"))
    resp = make_response()
    resp.headers["X-Chrome-ID-Consistency-Response"] = "action=SIGNIN,authuser=1,id=12345,email=test@test.com,authorization_code=FUCKFUCKFUCK"
    #resp.headers["X-Chrome-ID-Consistency-Response"] = "action=SIGNIN,authuser=1,id=12345,email=test@test.com,no_authorization_code=true"
    resp.headers["Location"] = url_for("enable_sync")
    return resp, 302
    #return redirect("/enable_sync",response = resp)

@app.route("/enable_sync")
def enable_sync():
    print(request.headers.get("X-Chrome-ID-Consistency-Request"))
    resp = make_response()
    resp.headers["X-Chrome-ID-Consistency-Response"] = "action=ENABLE_SYNC,authuser=1,id=12345,email=test@test.com"
    return resp

@app.route("/GetCheckConnectionInfo")
def GetCheckConnectionInfo():
    return jsonify({})

@app.route("/GetUserInfo")
def GetUserInfo():
    return "email=test@test.com\ndisplayEmail=test+display@test.com"

@app.route("/ListAccounts" , methods= [ "POST" ])
def ListAccounts():
    if request.method == 'POST':
        return '''["gaia.l.a.r", [""]]''' #test@test.com

@app.route("/oauth/multilogin", methods = [ "POST" ])
def oauth_multilogin():
    print(request.data)
    return "",401
    pass

# --sync-url
sync = Blueprint('sync-url',__name__ , url_prefix='/sync')
@sync.route("/", methods = ["GET","POST"])
def syncserver():
    print(request.data)
    print(request.headers)
    return ''
    pass

@sync.route("/command/", methods = ["POST"])
def synccommand():
    print(request.data)
    print(request.headers)
    return "",404

app.register_blueprint(sync)


# --oauth-account-manager-url
@app.route("/oam", methods = ["GET","POST"])
def oauthaccountmanager():
    return ''
    pass

# --google-apis-url https://127.0.0.1:5000/apis/ the ending slash is important!!!!
google_apis = Blueprint('google-apis-url',__name__ , url_prefix='/apis')


@google_apis.route("/oauth2/v4/token", methods = ["POST"])
def oauth2_v4_token():
    print(request.data)
    print(request.headers)
    return jsonify({"access_token":"test_access_token" , "refresh_token":"test_refresh_token", "expires_in": 9999})
    pass

@google_apis.route("/oauth2/v1/userinfo", methods = ["GET"])
def oauth2_v1_userinfo():
    print(request.data)
    return jsonify({"id":"12345","email":"test@test.com","id_token":"id_token_12345", "verified_email":"test+verified@test.com"})
    pass


app.register_blueprint(google_apis)


# --lso-url
@app.route("/lso", methods = ["GET","POST"])
def lso():
    return ''
    pass

# --google-url
@app.route("/google", methods = ["GET","POST"])
def google():
    return ''
    pass


WSGIRequestHandler.protocol_version = "HTTP/1.1"

