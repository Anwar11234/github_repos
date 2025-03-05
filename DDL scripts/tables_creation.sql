CREATE TABLE programmin_lang(
	language_name VARCHAR(255),
	number_of_repos INTEGER
);

CREATE TABLE organizations_stars(
	org_name VARCHAR(255),
	total_stars INTEGER
);

CREATE TABLE search_terms_relevance(
	search_term VARCHAR(255),
	repo_name VARCHAR(255),
	relevance_score INTEGER
);