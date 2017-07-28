import os
import sys
sys.path.append('../')
from utils import YearMonth, utcnow


year_months = [
               '2016-01', '2016-02', '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09',
               '2016-10', '2016-11', '2016-12']
for ym in year_months:
    os.system('python treat_sources_scope.py %s --reset_overlap' % YearMonth(ym))

