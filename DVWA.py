import urllib
import urllib2
import cookielib

class interface:
    def __init__(self):
        """Initialize the DVWA interface by logging in"""
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

        opener.addheaders = [('User-agent', 'DVWATesting')]
        urllib2.install_opener(opener)
        authentication_url = "http://localhost/DVWA/login.php"

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
        """Send SQL strings to the SQL page of DVWA"""
        sql_url = "http://localhost/DVWA/vulnerabilities/sqli/?"
        sql_params = {
                      "id" : sqlString,
                      "Submit" : "Submit"
        }
        sql_args = urllib.urlencode(sql_params)

        req = urllib2.Request(sql_url + sql_args + '#')
        resp = urllib2.urlopen(req)
        contents = resp.read()
        return self.check(contents)

    def check(self, contents):
        """Checks if there's a match with an error indicator"""
        error_indicators = ["error in your SQL syntax",
                            "mysql_num_rows() expects parameter 1",
                            "Query error", "SQL Error",
                            "Database Engine error", "Error has occurred",
                            "error occurred", "SQL Provider Error",
                            "Error executing statement", "database problem",
                            "No databse selected", "Unknown command",
                            "Query was empty", "Unknown error",
                            "Invalid use of NULL value"]
        y = str(contents)
        for error in error_indicators:
            if y.count(error):
                return True
        return False
