import azure.functions as func
import logging
import json
import requests
from datetime import datetime

app = func.FunctionApp()

# Databricks configuration
DATABRICKS_CONFIG = {
    'host': 'dbc-a3122420-cda0.cloud.databricks.com',
    'token': 'dapiebca5706c83ec913f50c0ab9e15e1e8c',
    'warehouse_id': 'b0b9bd45f760040f',
    'catalog': 'gebouwdossier',
    'schema': 'hackathon',
    'table': 'werkorders_storingen'
}

@app.route(route="order", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_order(req: func.HttpRequest) -> func.HttpResponse:
    """Receive order data and send it to Databricks"""
    logging.info('📦 Order endpoint called')
    
    try:
        order_data = req.get_json()
        
        logging.info(f"🚀 NIEUWE BESTELLING ONTVANGEN!")
        logging.info(f"Order ID: {order_data.get('orderId', 'N/A')}")
        logging.info(f"Model: {order_data.get('modelnaam', 'N/A')}")
        logging.info(f"Locatie: {order_data.get('locatie', 'N/A')}")
        logging.info(f"Prijs: €{order_data.get('prijs', 'N/A')}")
        
        # Create SQL statement for gebouwdossier.hackathon.werkorders_storingen
        # Columns: werkorder_id, storing_id, datum_aanmaak, omschrijving, status, uitvoerder, datum_uitgevoerd, contractid, asset_oud, asset_nieuw
        sql_statement = f"""
            INSERT INTO {DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.{DATABRICKS_CONFIG['table']} (
                werkorder_id, storing_id, datum_aanmaak, omschrijving, status, uitvoerder, datum_uitgevoerd, contractid, asset_oud, asset_nieuw
            ) VALUES (
                '{escape_sql(order_data.get('orderId', 'WO000'))}',
                '{escape_sql(order_data.get('storingId', 'S000'))}',
                '{order_data.get('datumAanmaak', '2024-06-10')}',
                '{escape_sql(order_data.get('omschrijving', 'Regelkast bestelling'))}',
                'open',
                '{escape_sql(order_data.get('uitvoerder', 'Onbekend'))}',
                NULL,
                {order_data.get('contractId', 1)},
                1000,
                2000
            )
        """
        
        # Databricks SQL API endpoint
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        request_body = {
            'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
            'statement': sql_statement,
            'wait_timeout': '30s'
        }
        
        logging.info(f"Sending to Databricks: {sql_statement}")
        
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
            logging.info(f"✅ Success! Databricks response: {result.get('status', {}).get('state')}")
            
            return func.HttpResponse(
                json.dumps({
                    'success': True,
                    'message': 'Order successfully saved to Databricks',
                    'orderId': order_data['orderId'],
                    'databricks_response': result
                }),
                status_code=200,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        else:
            error_text = response.text
            logging.error(f"❌ Databricks error: {response.status_code} - {error_text}")
            
            return func.HttpResponse(
                json.dumps({
                    'success': False,
                    'error': f"Databricks error: {response.status_code}",
                    'details': error_text
                }),
                status_code=response.status_code,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
            
    except Exception as e:
        logging.error(f"❌ Exception: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                'success': False,
                'error': str(e)
            }),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="werkorder/current", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_current_werkorder(req: func.HttpRequest) -> func.HttpResponse:
    """Get current werkorder with asset_oud=1000 and asset_nieuw=2000"""
    logging.info('📋 Get werkorder endpoint called')
    
    try:
        sql_statement = f"""
            SELECT * FROM {DATABRICKS_CONFIG['catalog']}.hackathon.werkorders_storingen
            WHERE asset_oud = 1000 AND asset_nieuw = 2000
        """
        
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        response = requests.post(
            api_url,
            headers={
                'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
                'Content-Type': 'application/json'
            },
            json={
                'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
                'statement': sql_statement,
                'wait_timeout': '30s'
            },
            timeout=35
        )
        
        if response.ok:
            result = response.json()
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                data_array = result.get('result', {}).get('data_array', [])
                if data_array and len(data_array) > 0:
                    columns = [col['name'] for col in result.get('manifest', {}).get('schema', {}).get('columns', [])]
                    werkorder_data = dict(zip(columns, data_array[0]))
                    logging.info(f"✅ Werkorder found: {werkorder_data.get('uitvoerder', 'Unknown')}")
                    return func.HttpResponse(
                        json.dumps({'success': True, 'werkorder': werkorder_data}),
                        status_code=200,
                        mimetype="application/json",
                        headers={"Access-Control-Allow-Origin": "*"}
                    )
        
        return func.HttpResponse(
            json.dumps({'success': False, 'error': 'Werkorder niet gevonden'}),
            status_code=404,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
        
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({'success': False, 'error': str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="asset/{asset_id:int}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_asset(req: func.HttpRequest) -> func.HttpResponse:
    """Get asset details from Databricks assets table"""
    logging.info('📦 Get asset endpoint called')
    
    try:
        asset_id = req.route_params.get('asset_id')
        
        sql_statement = f"""
            SELECT * FROM {DATABRICKS_CONFIG['catalog']}.hackathon.assets
            WHERE id = {asset_id}
        """
        
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        response = requests.post(
            api_url,
            headers={
                'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}",
                'Content-Type': 'application/json'
            },
            json={
                'warehouse_id': DATABRICKS_CONFIG['warehouse_id'],
                'statement': sql_statement,
                'wait_timeout': '30s'
            },
            timeout=35
        )
        
        if response.ok:
            result = response.json()
            if result.get('status', {}).get('state') == 'SUCCEEDED':
                data_array = result.get('result', {}).get('data_array', [])
                if data_array and len(data_array) > 0:
                    columns = [col['name'] for col in result.get('manifest', {}).get('schema', {}).get('columns', [])]
                    asset_data = dict(zip(columns, data_array[0]))
                    logging.info(f"✅ Asset {asset_id} found")
                    return func.HttpResponse(
                        json.dumps({'success': True, 'asset': asset_data}),
                        status_code=200,
                        mimetype="application/json",
                        headers={"Access-Control-Allow-Origin": "*"}
                    )
        
        return func.HttpResponse(
            json.dumps({'success': False, 'error': 'Asset niet gevonden'}),
            status_code=404,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
        
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({'success': False, 'error': str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="swap", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def swap_installation(req: func.HttpRequest) -> func.HttpResponse:
    """Receive swap request from werkorder app"""
    logging.info('🔄 Swap endpoint called')
    
    try:
        swap_data = req.get_json()
        
        logging.info("🔄 REGELKAST WISSEL ACTIE!")
        logging.info(f"Van: {swap_data.get('currentModel', 'N/A')}")
        logging.info(f"Naar: {swap_data.get('newModel', 'N/A')}")
        
        # Update assets
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Update asset 1000: RoomId = NULL
        update1_statement = f"UPDATE {DATABRICKS_CONFIG['catalog']}.hackathon.assets SET RoomId = NULL WHERE Id = 1000"
        
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
            logging.error(f"❌ Update 1 failed: {response1.status_code}")
            return func.HttpResponse(
                json.dumps({'success': False, 'error': f"Update 1 failed: {response1.status_code}"}),
                status_code=response1.status_code,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        logging.info("✅ Asset 1000 RoomId set to NULL")
        
        # Update asset 2000: RoomId = 21
        update2_statement = f"UPDATE {DATABRICKS_CONFIG['catalog']}.hackathon.assets SET RoomId = 21 WHERE Id = 2000"
        
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
            logging.error(f"❌ Update 2 failed: {response2.status_code}")
            return func.HttpResponse(
                json.dumps({'success': False, 'error': f"Update 2 failed: {response2.status_code}"}),
                status_code=response2.status_code,
                mimetype="application/json",
                headers={"Access-Control-Allow-Origin": "*"}
            )
        
        logging.info("✅ Asset 2000 RoomId set to 21")
        logging.info("✅ Swap complete!")
        
        return func.HttpResponse(
            json.dumps({
                'success': True,
                'message': 'Regelkast succesvol gewisseld',
                'timestamp': swap_data.get('timestamp')
            }),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
        
    except Exception as e:
        logging.error(f"❌ Error bij swap: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                'success': False,
                'error': str(e)
            }),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="reset", methods=["POST", "GET"], auth_level=func.AuthLevel.ANONYMOUS)
def reset_assets(req: func.HttpRequest) -> func.HttpResponse:
    """Reset assets to initial state"""
    logging.info('🔄 Reset endpoint called')
    
    try:
        api_url = f"https://{DATABRICKS_CONFIG['host']}/api/2.0/sql/statements"
        
        # Reset asset 1000: RoomId = 21
        reset1 = f"UPDATE {DATABRICKS_CONFIG['catalog']}.hackathon.assets SET RoomId = 21 WHERE Id = 1000"
        requests.post(
            api_url,
            headers={'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}", 'Content-Type': 'application/json'},
            json={'warehouse_id': DATABRICKS_CONFIG['warehouse_id'], 'statement': reset1, 'wait_timeout': '30s'},
            timeout=35
        )
        
        # Reset asset 2000: RoomId = NULL
        reset2 = f"UPDATE {DATABRICKS_CONFIG['catalog']}.hackathon.assets SET RoomId = NULL WHERE Id = 2000"
        requests.post(
            api_url,
            headers={'Authorization': f"Bearer {DATABRICKS_CONFIG['token']}", 'Content-Type': 'application/json'},
            json={'warehouse_id': DATABRICKS_CONFIG['warehouse_id'], 'statement': reset2, 'wait_timeout': '30s'},
            timeout=35
        )
        
        logging.info("✅ Reset complete!")
        
        return func.HttpResponse(
            json.dumps({
                'success': True,
                'message': 'Assets gereset naar initiële staat',
                'details': {'asset_1000_roomid': 21, 'asset_2000_roomid': None}
            }),
            status_code=200,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )
        
    except Exception as e:
        logging.error(f"❌ Error bij reset: {str(e)}")
        return func.HttpResponse(
            json.dumps({'success': False, 'error': str(e)}),
            status_code=500,
            mimetype="application/json",
            headers={"Access-Control-Allow-Origin": "*"}
        )


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    return func.HttpResponse(
        json.dumps({'status': 'healthy', 'service': 'heijmans-demo-api'}),
        status_code=200,
        mimetype="application/json",
        headers={"Access-Control-Allow-Origin": "*"}
    )


def escape_sql(value):
    """Escape single quotes for SQL"""
    return value.replace("'", "''")
