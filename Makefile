unit_test_coverage:
	coverage run -m pytest
	coverage html

unit_test_coverage_display:
	make unit_test_coverage
	xdg-open htmlcov/index.html