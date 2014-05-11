import urllib
import urllib2
import cookielib

class sender:
    def __init__(self):
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        opener.addheaders = [('User-agent', 'DVWATesting')]
        urllib2.install_opener(opener)
        authentication_url = "http://192.168.2.2/DVWA/login.php"

        login_params = {
                       "username" : "admin",
                       "password" : "password",
                       "Login": "Login"
                      }
        login_args = urllib.urlencode(login_params)

        req = urllib2.Request(authentication_url, login_args)

        resp = urllib2.urlopen(req)
        resp.read()

    def send(self, sqlString):
        sql_url = "http://192.168.2.2/DVWA/vulnerabilities/sqli/?"
        sql_params = {
                      "id" : sqlString,
                      "Submit" : "Submit"
        }
        sql_args = urllib.urlencode(sql_params)

        req = urllib2.Request(sql_url + sql_args + '#')
        resp = urllib2.urlopen(req)
        contents = resp.read()
        return contents
