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
    'schema': 'testsql',
    'table': 'test'
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
        
        # Create SQL statement for gebouwdossier.testsql.test
        # Based on the example: INSERT INTO gebouwdossier.testsql.test VALUES (2, 'Bob', 20.0), (3, 'Carol', 30.5);
        # Assuming columns are: id (INT), name (STRING), value (DECIMAL)
        sql_statement = f"""
            INSERT INTO {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.{DATABRICKS_CONFIG['table']} VALUES
            ({hash(order_data['orderId']) % 1000000}, '{escape_sql(order_data['modelnaam'])}', {order_data['prijs']})
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
        
        # TODO: Later hier Databricks update doen
        
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
