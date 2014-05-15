import urllib
import urllib2

class interface:
    def __init__(self):
        pass

    def send(self, sqlString):
        sql_url = "http://thegreatwall.web.engr.illinois.edu/quicksearch.php"
        sql_params = {
                      "search": sqlString,
                      "searchcourse" : "Search for Courses"
        }
        sql_args = urllib.urlencode(sql_params)

        req = urllib2.Request(sql_url, sql_args)
        resp = urllib2.urlopen(req)
        contents = resp.read()
        #print contents
        return self.check(contents)

    def check(self, contents):
        error_indicators = ["error in your SQL syntax",
                            "mysql_num_rows() expects parameter 1",
                            "Unknown column",
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
