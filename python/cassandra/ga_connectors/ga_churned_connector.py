from os import getenv
from ga_cassandra_connector import CassandraPersistance
from ga_main_connector import GoogleAnalytics


class GoogleAnalyticsChurn():
    def __init__(self):
        self.analytics = GoogleAnalytics()
        self.store = CassandraPersistance()
        self.store.prepare_statements(
            {'user_level': ['chu_users'], 'session_level': ['chu_sessions']})

    # Get churned users
    def store_chu_users(self):
        dimensions = ['dimension1', 'deviceCategory']
        metrics = ['sessions', 'sessionDuration', 'entrances',
                   'bounces', 'exits', 'pageValue', 'pageLoadTime', 'pageLoadSample']

        return self.analytics.run_report_and_store('chu_users', dimensions, metrics, self.store)

    # Get churned users with additional session data
    def store_chu_sessions(self):
        dimensions = ['dimension1', 'dimension2',
                      'sessionCount', 'daysSinceLastSession']
        metrics = ['sessions', 'pageviews', 'uniquePageviews',
                   'screenViews', 'hits', 'timeOnPage']

        return self.analytics.run_report_and_store('chu_sessions', dimensions, metrics, self.store)

    def run(self):
        self.analytics.authenticate()
        self.store_chu_users()
        self.store_chu_sessions()


if __name__ == '__main__':
    google_analytics = GoogleAnalyticsChurn()
    google_analytics.run()
