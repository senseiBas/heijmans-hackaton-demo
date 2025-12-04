"""
Databricks Proxy Server
Simple Flask server to bypass CORS restrictions for Databricks API calls
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Databricks configuration
DATABRICKS_CONFIG = {
    'host': 'dbc-a3122420-cda0.cloud.databricks.com',
    'token': 'dapiebca5706c83ec913f50c0ab9e15e1e8c',
    'warehouse_id': 'b0b9bd45f760040f',
    'catalog': 'gebouwdossier',
    'schema': 'hackathon',
    'table': 'werkorders_storingen'
}

@app.route('/api/order', methods=['POST'])
def create_order():
    """Receive order data and send it to Databricks"""
    try:
        order_data = request.json
        
        print("\n" + "="*60)
        print("🚀 NIEUWE BESTELLING ONTVANGEN!")
        print("="*60)
        print(f"Order ID: {order_data.get('orderId', 'N/A')}")
        print(f"Model: {order_data.get('modelnaam', 'N/A')}")
        print(f"Locatie: {order_data.get('locatie', 'N/A')}")
        print(f"Prijs: €{order_data.get('prijs', 'N/A')}")
        print("="*60)
        print(f"\nVolledige data:\n{json.dumps(order_data, indent=2)}")
        
        # Create SQL statement for gebouwdossier.hackathon.werkorders_storingen
        # Insert new record with asset_oud=1000 and asset_nieuw=2000
        sql_statement = f"""
            INSERT INTO {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.{DATABRICKS_CONFIG['table']} (
                werkorder_id, storing_id, datum_aanmaak, omschrijving, status, uitvoerder, datum_uitgevoerd, contractid, asset_oud, asset_nieuw
            ) VALUES (
                '{escape_sql(order_data.get('orderId', 'WO000'))}',
                '{escape_sql(order_data.get('storingId', 'S000'))}',
                '{order_data.get('datumAanmaak', '2025-12-04')}',
                '{escape_sql(order_data.get('omschrijving', 'Regelkast bestelling'))}',
                'open',
                '{escape_sql(order_data.get('uitvoerder', 'Marc'))}',
                NULL,
                {order_data.get('contractId', 1)},
                1000,
                2000
            )
        """
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Request body
        request_body = {
            'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
            'statement': sql_statement,
            'wait_timeout': '30s'
        }
        
        print(f"Sending to Databricks: {sql_statement}")
        
        # Make request to Databricks
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
            print(f"Success! Response: {json.dumps(result, indent=2)}")
            return jsonify({
                'success': True,
                'message': 'Order successfully saved to Databricks',
                'orderId': order_data['orderId'],
                'databricks_response': result
            }), 200
        else:
            error_text = response.text
            print(f"Error from Databricks: {response.status_code} - {error_text}")
            return jsonify({
                'success': False,
                'error': f"Databricks error: {response.status_code}",
                'details': error_text
            }), response.status_code
            
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def escape_sql(value):
    """Escape single quotes for SQL"""
    return value.replace("'", "''")

@app.route('/api/werkorder/current', methods=['GET'])
def get_current_werkorder():
    """Get current werkorder with asset_oud=1000 and asset_nieuw=2000"""
    try:
        print(f"\n📋 Ophalen huidige werkorder...")
        
        # SQL query to get werkorder with asset 1000 and 2000
        sql_statement = f"""
            SELECT * FROM {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.{DATABRICKS_CONFIG['table']}
            WHERE asset_oud = 1000 AND asset_nieuw = 2000
        """
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Request body
        request_body = {
            'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
            'statement': sql_statement,
            'wait_timeout': '30s'
        }
        
        print(f"Query: {sql_statement}")
        
        # Make request to Databricks
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
            
            # Extract data from Databricks response
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                data_array = result.get('result', {}).get('data_array', [])
                if data_array and len(data_array) > 0:
                    columns = [col['name'] for col in result.get('manifest', {}).get('schema', {}).get('columns', [])]
                    werkorder_data = dict(zip(columns, data_array[0]))
                    print(f"✅ Werkorder gevonden voor monteur: {werkorder_data.get('uitvoerder', 'Onbekend')}")
                    return jsonify({
                        'success': True,
                        'werkorder': werkorder_data
                    }), 200
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Geen werkorder gevonden'
                    }), 404
            else:
                return jsonify({
                    'success': False,
                    'error': 'Query niet succesvol'
                }), 500
        else:
            error_text = response.text
            print(f"❌ Error from Databricks: {response.status_code} - {error_text}")
            return jsonify({
                'success': False,
                'error': f"Databricks error: {response.status_code}",
                'details': error_text
            }), response.status_code
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/asset/<int:asset_id>', methods=['GET'])
def get_asset(asset_id):
    """Get asset details from Databricks assets table"""
    try:
        print(f"\n📦 Ophalen asset ID: {asset_id}")
        
        # SQL query to get asset by ID
        sql_statement = f"""
            SELECT * FROM {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.assets
            WHERE id = {asset_id}
        """
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Request body
        request_body = {
            'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
            'statement': sql_statement,
            'wait_timeout': '30s'
        }
        
        print(f"Query: {sql_statement}")
        
        # Make request to Databricks
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
            print(f"✅ Asset gevonden!")
            
            # Extract data from Databricks response
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                data_array = result.get('result', {}).get('data_array', [])
                if data_array and len(data_array) > 0:
                    # Assuming columns are returned in the manifest
                    columns = [col['name'] for col in result.get('manifest', {}).get('schema', {}).get('columns', [])]
                    asset_data = dict(zip(columns, data_array[0]))
                    return jsonify({
                        'success': True,
                        'asset': asset_data
                    }), 200
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Asset niet gevonden'
                    }), 404
            else:
                return jsonify({
                    'success': False,
                    'error': 'Query niet succesvol'
                }), 500
        else:
            error_text = response.text
            print(f"❌ Error from Databricks: {response.status_code} - {error_text}")
            return jsonify({
                'success': False,
                'error': f"Databricks error: {response.status_code}",
                'details': error_text
            }), response.status_code
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/swap', methods=['POST'])
def swap_installation():
    """Receive swap request from werkorder app"""
    try:
        swap_data = request.json
        
        print("\n" + "="*60)
        print("🔄 REGELKAST WISSEL ACTIE!")
        print("="*60)
        print(f"Van: {swap_data.get('currentModel', 'N/A')}")
        print(f"Naar: {swap_data.get('newModel', 'N/A')}")
        print(f"Locatie: {swap_data.get('location', 'N/A')}")
        print(f"Prijs nieuwe kast: €{swap_data.get('price', 'N/A')}")
        print("="*60)
        print(f"\nVolledige data:\n{json.dumps(swap_data, indent=2)}")
        print("="*60 + "\n")
        
        # Update RoomId voor beide assets
        # Asset 1000: RoomId = NULL (oude kast verwijderd)
        # Asset 2000: RoomId = 21 (nieuwe kast geïnstalleerd)
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Update 1: Set asset 1000 RoomId to NULL
        update1_statement = f"""
            UPDATE {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.assets 
            SET RoomId = NULL 
            WHERE Id = 1000
        """
        
        print(f"Executing update 1:\n{update1_statement}")
        
        response1 = requests.post(
            api_url,
            headers={
                'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
                'Content-Type': 'application/json'
            },
            json={
                'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
                'statement': update1_statement,
                'wait_timeout': '30s'
            },
            timeout=35
        )
        
        if not response1.ok:
            error_text = response1.text
            print(f"❌ Error bij update 1: {response1.status_code} - {error_text}")
            return jsonify({
                'success': False,
                'error': f"Update 1 failed: {response1.status_code}",
                'details': error_text
            }), response1.status_code
        
        print(f"✅ Asset 1000 RoomId set to NULL")
        
        # Update 2: Set asset 2000 RoomId to 21
        update2_statement = f"""
            UPDATE {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.assets 
            SET RoomId = 21 
            WHERE Id = 2000
        """
        
        print(f"Executing update 2:\n{update2_statement}")
        
        response2 = requests.post(
            api_url,
            headers={
                'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
                'Content-Type': 'application/json'
            },
            json={
                'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
                'statement': update2_statement,
                'wait_timeout': '30s'
            },
            timeout=35
        )
        
        if not response2.ok:
            error_text = response2.text
            print(f"❌ Error bij update 2: {response2.status_code} - {error_text}")
            return jsonify({
                'success': False,
                'error': f"Update 2 failed: {response2.status_code}",
                'details': error_text
            }), response2.status_code
        
        print(f"✅ Asset 2000 RoomId set to 21")
        print(f"✅ Alle assets succesvol geupdatet!")
        
        return jsonify({
            'success': True,
            'message': 'Regelkast succesvol gewisseld',
            'timestamp': swap_data.get('timestamp')
        }), 200
        
    except Exception as e:
        print(f"\n❌ Error bij swap: {str(e)}\n")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'databricks-proxy'}), 200

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Databricks Proxy Server Starting...")
    print("=" * 60)
    print(f"Server will run on: http://10.98.2.112:5000")
    print(f"Local access:    http://localhost:5000")
    print(f"Network access:  http://10.98.2.112:5000")
    print(f"Databricks host: {DATABRICKS_CONFIG['host']}")
    print(f"Warehouse ID: {DATABRICKS_CONFIG['warehouse_id']}")
    print("=" * 60)
    print("⚠️  Make sure Windows Firewall allows port 5000!")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
