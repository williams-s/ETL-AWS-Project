#!/bin/bash


SQL_DIR="../sql_queries"
OUTPUT_LOCATION="s3://netflix-analytics-williams2/athena_results/"

if [ ! -d "$SQL_DIR" ]; then
    echo "Dossier $SQL_DIR introuvable"
    exit 1
fi

if [ -f "$SQL_DIR/movies_stats.sql" ]; then
    SQL_CONTENT=$(cat "$SQL_DIR/movies_stats.sql")
    
    QUERY_ID=$(aws athena start-query-execution \
        --query-string "$SQL_CONTENT" \
        --work-group primary \
	--query-execution-context "Database=netflix_database" \
        --result-configuration "OutputLocation=$OUTPUT_LOCATION" \
        --query "QueryExecutionId" \
        --output text)
    
    echo "movies_stats.sql envoyé (ID: $QUERY_ID)"
    sleep 5  
else
    echo "ovies_stats.sql non trouvé"
fi

for SQL_FILE in "$SQL_DIR"/*.sql; do
    if [ -f "$SQL_FILE" ] && [ "$(basename "$SQL_FILE")" != "movies_stats.sql" ]; then
        FILENAME=$(basename "$SQL_FILE")
        
        SQL_CONTENT=$(cat "$SQL_FILE")
        
        QUERY_ID=$(aws athena start-query-execution \
            --query-string "$SQL_CONTENT" \
            --work-group primary \
	    --query-execution-context "Database=netflix_database" \
            --result-configuration "OutputLocation=$OUTPUT_LOCATION" \
            --query "QueryExecutionId" \
            --output text)
        echo "$FILENAME envoyé (ID: $QUERY_ID)"
        sleep 2
    fi
done

echo "Fichiers SQL exécutés"
