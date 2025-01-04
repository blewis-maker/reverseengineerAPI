import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ArcGISUpdater:
    def __init__(self):
        # Load credentials from environment variables
        self.base_url = os.getenv('ARCGIS_URL')
        self.username = os.getenv('ARCGIS_USERNAME')
        self.password = os.getenv('ARCGIS_PASSWORD')
        
        if not all([self.base_url, self.username, self.password]):
            raise ValueError("Missing required ArcGIS Enterprise credentials in environment variables")
        
        # Get token
        self.token = self._get_token()
        print(f"Successfully connected to ArcGIS Enterprise as {self.username}")
        
        # Store feature service URLs using the correct layer IDs
        self.feature_services = {
            'poles': f"{self.base_url}/1",
            'connections': f"{self.base_url}/3",
            'anchors': f"{self.base_url}/2"
        }
    
    def _get_token(self):
        """Get an authentication token from ArcGIS Enterprise Portal"""
        token_url = "https://gis.clearnetworx.com/portal/sharing/rest/generateToken"
        username = "brandan.lewis"  # Try with username format
        
        params = {
            'f': 'json',
            'username': username,
            'password': self.password,
            'client': 'referer',
            'referer': 'https://www.arcgis.com',
            'expiration': 60
        }
        
        print(f"Debug - Using credentials - Username: {username}")
        print(f"Debug - Token URL: {token_url}")
        
        try:
            response = requests.post(token_url, data=params, verify=False)
            response.raise_for_status()
            result = response.json()
            
            if 'error' in result:
                raise ValueError(f"Error getting token: {result}")
            
            return result['token']
        except Exception as e:
            raise ValueError(f"Failed to get token: {str(e)}")
    
    def update_features(self, layer_name, features):
        """Update features in a specific layer using REST API"""
        if layer_name not in self.feature_services:
            raise ValueError(f"Feature service for {layer_name} not found")
            
        service_url = self.feature_services[layer_name]
        
        # Update features in chunks to handle rate limits
        chunk_size = 100
        for i in range(0, len(features), chunk_size):
            chunk = features[i:i + chunk_size]
            
            try:
                # Prepare the update request
                update_url = f"{service_url}/updateFeatures"
                params = {
                    'f': 'json',
                    'token': self.token,
                    'features': json.dumps(chunk)
                }
                
                response = requests.post(update_url, data=params)
                response.raise_for_status()
                result = response.json()
                
                if 'error' in result:
                    print(f"Error updating features: {result['error']}")
                else:
                    print(f"Successfully updated {len(chunk)} features in {layer_name}")
                    
            except Exception as e:
                print(f"Error updating chunk: {str(e)}")
                # Try to refresh token if it might have expired
                if 'token' in str(e).lower():
                    try:
                        self.token = self._get_token()
                        print("Token refreshed, retrying update...")
                        # Retry the update with new token
                        params['token'] = self.token
                        response = requests.post(update_url, data=params)
                        response.raise_for_status()
                        result = response.json()
                        if 'error' not in result:
                            print(f"Successfully updated {len(chunk)} features in {layer_name} after token refresh")
                    except Exception as retry_error:
                        print(f"Error retrying update after token refresh: {str(retry_error)}")
    
    def process_shapefile(self, shapefile_path, layer_name):
        """Process a shapefile and update the corresponding feature service"""
        try:
            # Read shapefile
            gdf = gpd.read_file(shapefile_path)
            
            # Convert features to ArcGIS JSON format
            features = []
            for idx, row in gdf.iterrows():
                feature = {
                    'geometry': json.loads(row.geometry.json),
                    'attributes': {
                        col: row[col] for col in gdf.columns if col != 'geometry'
                    }
                }
                features.append(feature)
            
            # Update features
            self.update_features(layer_name, features)
            
        except Exception as e:
            print(f"Error processing shapefile {shapefile_path}: {str(e)}")
    
    def process_master_zip(self, master_zip_path):
        """Process all layers in the master zip file"""
        import tempfile
        import zipfile
        
        with zipfile.ZipFile(master_zip_path, 'r') as zip_ref:
            with tempfile.TemporaryDirectory() as tmpdir:
                zip_ref.extractall(tmpdir)
                
                # Process each layer
                layer_files = {
                    'poles': 'poles.shp',
                    'connections': 'connections.shp',
                    'anchors': 'anchors.shp'
                }
                
                for layer_name, filename in layer_files.items():
                    shapefile_path = os.path.join(tmpdir, filename)
                    if os.path.exists(shapefile_path):
                        print(f"\nProcessing {layer_name}...")
                        self.process_shapefile(shapefile_path, layer_name)
                    else:
                        print(f"Warning: {filename} not found in master zip") 