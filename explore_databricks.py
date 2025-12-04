"""
Databricks Explorer - Quick test script om de database te verkennen
"""
import requests
import json

# Databricks configuratie
DATABRICKS_CONFIG = {
    'host': 'dbc-a3122420-cda0.cloud.databricks.com',
    'token': 'dapiebca5706c83ec913f50c0ab9e15e1e8c',
    'warehouse_id': 'b0b9bd45f760040f'
}

def run_query(sql):
    """Run een SQL query op Databricks en print het resultaat"""
    api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
    
    request_body = {
        'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
        'statement': sql,
        'wait_timeout': '30s'
    }
    
    print(f"\n{'='*60}")
    print(f"SQL Query: {sql}")
    print(f"{'='*60}")
    
    response = requests.post(
        api_url,
        headers={
            'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
            'Content-Type': 'application/json'
        },
        json=request_body,
        timeout=35
    )
    
    if response.ok:
        result = response.json()
        
        # Check status
        if result.get('status', {}).get('state') == 'SUCCEEDED':
            print("\n✅ Query succeeded!")
            
            # Print results
            if 'result' in result and 'data_array' in result['result']:
                data = result['result']['data_array']
                columns = [col['name'] for col in result['result'].get('manifest', {}).get('schema', {}).get('columns', [])]
                
                print(f"\nKolommen: {columns}")
                print(f"\nAantal rijen: {len(data)}")
                print("\nData:")
                print("-" * 60)
                for row in data:
                    print(row)
                print("-" * 60)
            else:
                print("\n(Geen data geretourneerd - query was waarschijnlijk een DDL statement)")
                
        else:
            print(f"\n❌ Query failed!")
            error = result.get('status', {}).get('error', {})
            print(f"Error: {error.get('message', 'Unknown error')}")
            
        return result
    else:
        print(f"\n❌ API call failed: {response.status_code}")
        print(response.text)
        return None

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🔍 DATABRICKS EXPLORER")
    print("="*60)
    
    # Test 1: Beschrijf de tabel structuur
    print("\n\n📊 TEST 1: Tabel structuur ophalen")
    run_query("DESCRIBE gebouwdossier.testsql.test")
    
    # Test 2: Haal alle data op
    print("\n\n📊 TEST 2: Alle data ophalen")
    run_query("SELECT * FROM gebouwdossier.testsql.test")
    
    # Test 3: Laatste 5 rijen
    print("\n\n📊 TEST 3: Laatste 5 rijen")
    run_query("SELECT * FROM gebouwdossier.testsql.test ORDER BY id DESC LIMIT 5")
    
    print("\n\n" + "="*60)
    print("✅ Exploration complete!")
    print("="*60)
