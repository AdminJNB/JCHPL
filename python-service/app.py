from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME', 'jchpl_mis'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'Mahaveer@123')
    )

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'python-data-processor'})

@app.route('/api/analytics/revenue-forecast', methods=['GET'])
def revenue_forecast():
    """Generate revenue forecast based on historical data"""
    try:
        months_ahead = int(request.args.get('months', 3))
        conn = get_db_connection()
        
        query = """
            SELECT 
                DATE_TRUNC('month', created_at) as month,
                SUM(net_amount) as revenue
            FROM revenues
            WHERE created_at >= NOW() - INTERVAL '12 months'
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if len(df) < 3:
            return jsonify({'error': 'Insufficient data for forecasting'}), 400
        
        # Simple linear regression forecast
        df['month_num'] = range(len(df))
        X = df['month_num'].values
        y = df['revenue'].values.astype(float)
        
        # Calculate linear regression coefficients
        n = len(X)
        sum_x = np.sum(X)
        sum_y = np.sum(y)
        sum_xy = np.sum(X * y)
        sum_x2 = np.sum(X ** 2)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
        intercept = (sum_y - slope * sum_x) / n
        
        # Generate forecasts
        last_month_num = X[-1]
        forecasts = []
        last_date = df['month'].iloc[-1]
        
        for i in range(1, months_ahead + 1):
            future_month_num = last_month_num + i
            predicted_revenue = intercept + slope * future_month_num
            forecast_date = last_date + pd.DateOffset(months=i)
            
            forecasts.append({
                'month': forecast_date.strftime('%Y-%m'),
                'predicted_revenue': max(0, round(predicted_revenue, 2)),
                'confidence_lower': max(0, round(predicted_revenue * 0.85, 2)),
                'confidence_upper': round(predicted_revenue * 1.15, 2)
            })
        
        return jsonify({
            'historical': df[['month', 'revenue']].to_dict('records'),
            'forecasts': forecasts,
            'trend': 'increasing' if slope > 0 else 'decreasing',
            'monthly_growth_rate': round((slope / y.mean()) * 100, 2)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/expense-anomalies', methods=['GET'])
def expense_anomalies():
    """Detect expense anomalies using statistical analysis"""
    try:
        expense_head_id = request.args.get('expense_head_id')
        threshold = float(request.args.get('threshold', 2.5))  # Standard deviations
        
        conn = get_db_connection()
        
        base_query = """
            SELECT 
                e.id,
                e.expense_date,
                e.amount,
                eh.name as expense_head_name,
                t.name as team_name
            FROM expenses e
            JOIN expense_heads eh ON e.expense_head_id = eh.id
            LEFT JOIN teams t ON e.team_id = t.id
            WHERE e.expense_date >= NOW() - INTERVAL '6 months'
        """
        
        if expense_head_id:
            base_query += f" AND e.expense_head_id = '{expense_head_id}'"
        
        df = pd.read_sql(base_query, conn)
        conn.close()
        
        if len(df) < 5:
            return jsonify({'anomalies': [], 'message': 'Insufficient data for analysis'})
        
        # Group by expense head and calculate statistics
        anomalies = []
        for head_name in df['expense_head_name'].unique():
            head_df = df[df['expense_head_name'] == head_name]
            
            if len(head_df) < 3:
                continue
                
            mean = head_df['amount'].mean()
            std = head_df['amount'].std()
            
            if std == 0:
                continue
            
            # Find anomalies
            for _, row in head_df.iterrows():
                z_score = (row['amount'] - mean) / std
                if abs(z_score) > threshold:
                    anomalies.append({
                        'id': str(row['id']),
                        'expense_date': row['expense_date'].strftime('%Y-%m-%d') if pd.notnull(row['expense_date']) else None,
                        'amount': float(row['amount']),
                        'expense_head': row['expense_head_name'],
                        'team': row['team_name'],
                        'z_score': round(z_score, 2),
                        'average': round(mean, 2),
                        'std_dev': round(std, 2),
                        'deviation_percentage': round(((row['amount'] - mean) / mean) * 100, 2)
                    })
        
        # Sort by z_score descending
        anomalies.sort(key=lambda x: abs(x['z_score']), reverse=True)
        
        return jsonify({
            'anomalies': anomalies[:20],  # Return top 20 anomalies
            'total_analyzed': len(df),
            'threshold_used': threshold
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/client-profitability', methods=['GET'])
def client_profitability():
    """Calculate detailed client profitability metrics"""
    try:
        service_period_id = request.args.get('service_period_id')
        
        conn = get_db_connection()
        
        # Get revenue by client
        revenue_query = """
            SELECT 
                c.id as client_id,
                c.name as client_name,
                SUM(r.net_amount) as total_revenue,
                COUNT(r.id) as transaction_count
            FROM clients c
            LEFT JOIN revenues r ON c.id = r.client_id
        """
        if service_period_id:
            revenue_query += f" WHERE r.service_period_id = '{service_period_id}'"
        revenue_query += " GROUP BY c.id, c.name"
        
        revenue_df = pd.read_sql(revenue_query, conn)
        
        # Get expenses allocated to clients
        expense_query = """
            SELECT 
                c.id as client_id,
                SUM(e.amount) as direct_expenses
            FROM clients c
            LEFT JOIN expenses e ON c.id = e.client_id
            GROUP BY c.id
        """
        expense_df = pd.read_sql(expense_query, conn)
        
        # Get team allocation expenses
        allocation_query = """
            SELECT 
                tca.client_id,
                SUM(e.amount * tca.allocation_percentage / 100) as allocated_expenses
            FROM team_client_allocations tca
            JOIN expenses e ON tca.team_id = e.team_id
            GROUP BY tca.client_id
        """
        allocation_df = pd.read_sql(allocation_query, conn)
        
        conn.close()
        
        # Merge data
        result_df = revenue_df.merge(expense_df, on='client_id', how='left')
        result_df = result_df.merge(allocation_df, on='client_id', how='left')
        
        # Calculate profitability metrics
        result_df['direct_expenses'] = result_df['direct_expenses'].fillna(0)
        result_df['allocated_expenses'] = result_df['allocated_expenses'].fillna(0)
        result_df['total_expenses'] = result_df['direct_expenses'] + result_df['allocated_expenses']
        result_df['profit'] = result_df['total_revenue'].fillna(0) - result_df['total_expenses']
        result_df['profit_margin'] = np.where(
            result_df['total_revenue'] > 0,
            (result_df['profit'] / result_df['total_revenue'] * 100).round(2),
            0
        )
        
        # Classify clients
        def classify_client(row):
            if row['profit_margin'] >= 30:
                return 'High Performer'
            elif row['profit_margin'] >= 15:
                return 'Good'
            elif row['profit_margin'] >= 0:
                return 'Average'
            else:
                return 'Loss Making'
        
        result_df['classification'] = result_df.apply(classify_client, axis=1)
        
        # Convert to records
        records = result_df.to_dict('records')
        
        # Add summary stats
        total_revenue = result_df['total_revenue'].sum()
        total_expenses = result_df['total_expenses'].sum()
        
        return jsonify({
            'clients': records,
            'summary': {
                'total_clients': len(records),
                'total_revenue': round(total_revenue, 2),
                'total_expenses': round(total_expenses, 2),
                'total_profit': round(total_revenue - total_expenses, 2),
                'average_profit_margin': round(result_df['profit_margin'].mean(), 2),
                'high_performers': len(result_df[result_df['classification'] == 'High Performer']),
                'loss_making': len(result_df[result_df['classification'] == 'Loss Making'])
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/revenue-distribution', methods=['GET'])
def revenue_distribution():
    """Analyze revenue distribution across different dimensions"""
    try:
        conn = get_db_connection()
        
        # By service type
        service_query = """
            SELECT 
                st.name as service_type,
                SUM(r.net_amount) as revenue,
                COUNT(r.id) as count
            FROM revenues r
            JOIN service_types st ON r.service_type_id = st.id
            GROUP BY st.name
            ORDER BY revenue DESC
        """
        service_df = pd.read_sql(service_query, conn)
        
        # By month
        monthly_query = """
            SELECT 
                TO_CHAR(r.created_at, 'YYYY-MM') as month,
                SUM(r.net_amount) as revenue
            FROM revenues r
            WHERE r.created_at >= NOW() - INTERVAL '12 months'
            GROUP BY TO_CHAR(r.created_at, 'YYYY-MM')
            ORDER BY month
        """
        monthly_df = pd.read_sql(monthly_query, conn)
        
        # By interstate status
        gst_query = """
            SELECT 
                CASE WHEN is_interstate THEN 'Interstate (IGST)' ELSE 'Intrastate (CGST/SGST)' END as gst_type,
                SUM(net_amount) as revenue,
                SUM(igst) as igst,
                SUM(cgst) as cgst,
                SUM(sgst) as sgst
            FROM revenues
            GROUP BY is_interstate
        """
        gst_df = pd.read_sql(gst_query, conn)
        
        conn.close()
        
        return jsonify({
            'by_service_type': service_df.to_dict('records'),
            'by_month': monthly_df.to_dict('records'),
            'by_gst_type': gst_df.to_dict('records')
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/team-performance', methods=['GET'])
def team_performance():
    """Analyze team/reviewer performance metrics"""
    try:
        conn = get_db_connection()
        
        # Reviewer revenue contribution
        reviewer_query = """
            SELECT 
                t.id as reviewer_id,
                t.name as reviewer_name,
                SUM(r.net_amount * rra.allocation_percentage / 100) as allocated_revenue,
                COUNT(DISTINCT r.id) as revenue_entries
            FROM teams t
            JOIN revenue_reviewer_allocations rra ON t.id = rra.reviewer_id
            JOIN revenues r ON rra.revenue_id = r.id
            WHERE t.is_reviewer = true
            GROUP BY t.id, t.name
            ORDER BY allocated_revenue DESC
        """
        reviewer_df = pd.read_sql(reviewer_query, conn)
        
        # Team expenses
        team_expense_query = """
            SELECT 
                t.id as team_id,
                t.name as team_name,
                t.expense_type,
                SUM(e.amount) as total_expenses,
                COUNT(e.id) as expense_count
            FROM teams t
            LEFT JOIN expenses e ON t.id = e.team_id
            GROUP BY t.id, t.name, t.expense_type
            ORDER BY total_expenses DESC
        """
        team_expense_df = pd.read_sql(team_expense_query, conn)
        
        conn.close()
        
        return jsonify({
            'reviewer_performance': reviewer_df.to_dict('records'),
            'team_expenses': team_expense_df.to_dict('records')
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/process-bulk-revenue', methods=['POST'])
def process_bulk_revenue():
    """Process bulk revenue data upload"""
    try:
        data = request.get_json()
        
        if not data or 'records' not in data:
            return jsonify({'error': 'No records provided'}), 400
        
        records = data['records']
        processed = []
        errors = []
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        for idx, record in enumerate(records):
            try:
                # Validate required fields
                required = ['client_name', 'service_type', 'service_period', 'gross_amount']
                missing = [f for f in required if f not in record or not record[f]]
                
                if missing:
                    errors.append({'row': idx + 1, 'error': f'Missing fields: {", ".join(missing)}'})
                    continue
                
                # Look up client
                cursor.execute("SELECT id FROM clients WHERE LOWER(name) = LOWER(%s)", (record['client_name'],))
                client = cursor.fetchone()
                if not client:
                    errors.append({'row': idx + 1, 'error': f'Client not found: {record["client_name"]}'})
                    continue
                
                # Look up service type
                cursor.execute("SELECT id, hsn_code, gst_rate FROM service_types WHERE LOWER(name) = LOWER(%s)", (record['service_type'],))
                service_type = cursor.fetchone()
                if not service_type:
                    errors.append({'row': idx + 1, 'error': f'Service type not found: {record["service_type"]}'})
                    continue
                
                # Look up service period
                cursor.execute("SELECT id FROM service_periods WHERE LOWER(name) = LOWER(%s)", (record['service_period'],))
                service_period = cursor.fetchone()
                if not service_period:
                    errors.append({'row': idx + 1, 'error': f'Service period not found: {record["service_period"]}'})
                    continue
                
                # Look up billing name (optional)
                billing_name_id = None
                if record.get('billing_name'):
                    cursor.execute("SELECT id FROM billing_names WHERE LOWER(name) = LOWER(%s)", (record['billing_name'],))
                    billing_name = cursor.fetchone()
                    if billing_name:
                        billing_name_id = billing_name['id']
                
                # Calculate GST
                gross_amount = float(record['gross_amount'])
                discount = float(record.get('discount', 0))
                is_interstate = record.get('is_interstate', False)
                gst_rate = service_type['gst_rate'] or 18
                
                taxable_amount = gross_amount - discount
                if is_interstate:
                    igst = taxable_amount * (gst_rate / 100)
                    cgst = sgst = 0
                else:
                    igst = 0
                    cgst = sgst = taxable_amount * (gst_rate / 200)
                
                net_amount = taxable_amount + igst + cgst + sgst
                
                # Generate unique key
                unique_key = f"{record['client_name']}|{record['service_type']}|{record['service_period']}".lower().replace(' ', '-')
                
                processed.append({
                    'client_id': client['id'],
                    'billing_name_id': billing_name_id,
                    'service_type_id': service_type['id'],
                    'service_period_id': service_period['id'],
                    'gross_amount': gross_amount,
                    'discount': discount,
                    'is_interstate': is_interstate,
                    'hsn_code': service_type['hsn_code'],
                    'igst': round(igst, 2),
                    'cgst': round(cgst, 2),
                    'sgst': round(sgst, 2),
                    'net_amount': round(net_amount, 2),
                    'unique_key': unique_key
                })
                
            except Exception as e:
                errors.append({'row': idx + 1, 'error': str(e)})
        
        conn.close()
        
        return jsonify({
            'processed': processed,
            'errors': errors,
            'summary': {
                'total_records': len(records),
                'successful': len(processed),
                'failed': len(errors)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PYTHON_SERVICE_PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)
