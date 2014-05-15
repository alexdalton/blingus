import urllib
import urllib2

class interface:
    def __init__(self):
        """Intentionally empty"""
        pass

    def send(self, sqlString):
        """Send to the search interface of ZMDB"""
        # Change url and params for another zmdb interface
        sql_url = "http://www.zmdb.org/search/"
        sql_params = {
                      "from": "items",
                      "search": sqlString,
                      "": "search!"
        }
        sql_args = urllib.urlencode(sql_params)

        req = urllib2.Request(sql_url, sql_args)
        resp = urllib2.urlopen(req)
        contents = resp.read()
        #print contents
        return self.check(contents)

    def check(self, contents):
        """Check if there's a match with an error indicator"""
        error_indicators = ["error in your SQL syntax",
                            "mysql_num_rows() expects parameter 1",
                            "mysql_fetch_row() expects parameter 1",
                            "Unknown column", "Unknown table"
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
