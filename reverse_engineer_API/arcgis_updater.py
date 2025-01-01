import os
import json
import requests
from datetime import datetime
import geopandas as gpd
from arcgis.gis import GIS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ArcGISUpdater:
    def __init__(self):
        # Load credentials from environment variables
        self.portal_url = os.getenv('ARCGIS_PORTAL_URL')
        self.username = os.getenv('ARCGIS_USERNAME')
        self.password = os.getenv('ARCGIS_PASSWORD')
        
        if not all([self.portal_url, self.username, self.password]):
            raise ValueError("Missing required ArcGIS Enterprise credentials in environment variables")
        
        # Initialize GIS connection
        self.gis = GIS(self.portal_url, self.username, self.password)
        print(f"Successfully connected to ArcGIS Enterprise as {self.username}")
        
        # Store feature service URLs after creation
        self.feature_services = {}
    
    def create_feature_services(self):
        """Create feature services for poles, connections, and anchors if they don't exist"""
        
        # Define schemas for each feature type
        schemas = {
            'poles': {
                'geometry_type': 'esriGeometryPoint',
                'fields': [
                    {'name': 'job_name', 'type': 'esriFieldTypeString', 'length': 255},
                    {'name': 'job_status', 'type': 'esriFieldTypeString', 'length': 50},
                    {'name': 'mr_status', 'type': 'esriFieldTypeString', 'length': 50},
                    {'name': 'utility', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'completed', 'type': 'esriFieldTypeString', 'length': 50},
                    {'name': 'pole_tag', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'poa_ht', 'type': 'esriFieldTypeString', 'length': 50},
                    {'name': 'last_editor', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'last_edit', 'type': 'esriFieldTypeString', 'length': 100}
                ]
            },
            'connections': {
                'geometry_type': 'esriGeometryPolyline',
                'fields': [
                    {'name': 'conn_id', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'conn_type', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'att_height', 'type': 'esriFieldTypeString', 'length': 50},
                    {'name': 'node_id_1', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'node_id_2', 'type': 'esriFieldTypeString', 'length': 100}
                ]
            },
            'anchors': {
                'geometry_type': 'esriGeometryPoint',
                'fields': [
                    {'name': 'anch_spec', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'job_id', 'type': 'esriFieldTypeString', 'length': 100},
                    {'name': 'anchor_type', 'type': 'esriFieldTypeString', 'length': 100}
                ]
            }
        }
        
        for layer_name, schema in schemas.items():
            try:
                # Check if service already exists
                existing_items = self.gis.content.search(
                    f'title:"KatapultPro_{layer_name}" AND type:Feature Service',
                    item_type="Feature Service"
                )
                
                if existing_items:
                    print(f"Feature service for {layer_name} already exists")
                    self.feature_services[layer_name] = existing_items[0].url
                    continue
                
                # Create new feature service
                service_create_params = {
                    'name': f'KatapultPro_{layer_name}',
                    'serviceDescription': f'KatapultPro {layer_name} data',
                    'hasStaticData': False,
                    'maxRecordCount': 10000,
                    'supportedQueryFormats': 'JSON',
                    'capabilities': 'Create,Delete,Query,Update,Editing',
                    'description': f'Feature service for KatapultPro {layer_name}',
                    'copyrightText': f'Generated on {datetime.now().strftime("%Y-%m-%d")}',
                    'initialExtent': {
                        'xmin': -180,
                        'ymin': -90,
                        'xmax': 180,
                        'ymax': 90,
                        'spatialReference': {'wkid': 4326}
                    },
                    'spatialReference': {'wkid': 4326},
                    'layers': [{
                        'name': layer_name,
                        'type': 'Feature Layer',
                        'displayField': 'OBJECTID',
                        'geometryType': schema['geometry_type'],
                        'hasM': False,
                        'hasZ': False,
                        'fields': [
                            {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID'},
                            *schema['fields']
                        ]
                    }]
                }
                
                # Create the service
                new_service = self.gis.content.create_service(
                    f'KatapultPro_{layer_name}',
                    create_params=service_create_params
                )
                
                print(f"Successfully created feature service for {layer_name}")
                self.feature_services[layer_name] = new_service.url
                
            except Exception as e:
                print(f"Error creating feature service for {layer_name}: {str(e)}")
    
    def update_features(self, layer_name, features):
        """Update features in a specific layer"""
        if layer_name not in self.feature_services:
            raise ValueError(f"Feature service for {layer_name} not found")
            
        service_url = self.feature_services[layer_name]
        
        # Get token
        token = self.gis._con.token
        
        # Update features in chunks to handle rate limits
        chunk_size = 100
        for i in range(0, len(features), chunk_size):
            chunk = features[i:i + chunk_size]
            
            try:
                response = requests.post(
                    f"{service_url}/updateFeatures",
                    params={
                        'f': 'json',
                        'token': token
                    },
                    json={
                        'features': chunk
                    }
                )
                
                result = response.json()
                if 'error' in result:
                    print(f"Error updating features: {result['error']}")
                else:
                    print(f"Successfully updated {len(chunk)} features in {layer_name}")
                    
            except Exception as e:
                print(f"Error updating chunk: {str(e)}")
    
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