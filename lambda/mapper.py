def map_third_parties_from_view(selected_company, db_rows):
    # Mapping logic based on what view was selected
    COMPANY_ID_FIELD_MAP = {
        "CASABLANCA": "Id_Cliente_ERP_Casablanca__c",
        "ITALCOL": "Id_Cliente_ERP_Balanceados__c",
        "SANTA_REYES": "Id_Cliente_ERP_Italhuevo__c"
    }
    client_id_field_name = COMPANY_ID_FIELD_MAP.get(selected_company)
        
    mapped_list = []

    for idx, row in enumerate(db_rows):
        # Basic required fields
        row_body = {
            client_id_field_name: safe_get(row, "f200_nit", idx),
            "AccountNumber": safe_get(row, "f200_nit", idx),
            "Type": "Customer",
            "Industry": "Retail",
            "ShippingCity": safe_get(row, "f013_descripcion", idx),
            "ShippingState": safe_get(row, "f012_descripcion", idx),
            "ShippingCountry": safe_get(row, "f011_descripcion", idx),
            "ShippingStreet": safe_get(row, "f015_direccion1", idx),
            "cu_CIIU__c": safe_get(row, "f200_id_ciiu", idx),
            "cu_Telefono_Informativo__c": safe_get(row, "f015_Telefono", idx),
            "cu_Correo_Electronico_Informativo__c": safe_get(row, "f015_email", idx), # Adicionar
            "cu_tipo_de_documento_id__c": safe_get(row, "f203_descripcion", idx), # Tipo de documento
            "Nombre_Comercial__c": safe_get(row,"f200_nombre_est", idx),
        }

        tipo_ident = safe_get(row, "f200_ind_tipo_tercero", idx) #f200_id_tipo_ident

        if tipo_ident == 2: # Entidad juridica
            row_body["Name"] = safe_get(row, "f200_razon_social", idx)
            row_body["cu_colaborador_grupo_italcol__c"] = False
            #row_body["Nombre_Comercial__c"] = safe_get(row,"f200_nombre_est",idx) #Aplica para todos
        elif tipo_ident : # 
            row_body["FirstName"] = safe_get(row, "f200_nombres", idx)
            row_body["LastName"] = f"{safe_get(row, 'f200_apellido1', idx)} {safe_get(row, 'f200_apellido2', idx)}"
            row_body["Salutation"] = "Sr(a)."
            row_body["cu_colaborador_grupo_italcol__c"] = True

            if safe_get(row, "f200_ind_empleado", idx) == 1:
                row_body["cu_compania__c"] = selected_company # cu_compania__c

        if selected_company == "ITALCOL":
            row_body["cu_Clasificacion_de_Cliente__c"] =  safe_get(row,"ClasificacionCliente", idx) # Solo Italcol
            row_body["cu_Tipo_Cuenta__c"] = "Cliente" #Solo italcol

        mapped_list.append(row_body)
    return mapped_list

def map_retail_stores_from_view(selected_company, db_rows):
    # Select which field names will be used based on the selected company
    COMPANY_FIELD_MAPS = {
        "CASABLANCA": {
            "client_id_field_name": "ti_Id_PV_ERP_Casablanca__c",
            "email_field_name": "ti_correos_electronicos_Casablanca__c",
            "salesman_field_name": "ti_Nombre_Vendedor_Casablanca__c",
            "salesman_code_field_name": "ti_Codigo_Vendedor_Casablanca__c"
        },
        "ITALCOL": {
            "client_id_field_name": "ti_Id_PV_ERP_Balanceados__c",
            "email_field_name": "ti_correos_electronicos_Balanceados",
            "salesman_field_name": "ti_Nombre_Vendedor_Balanceados__c",
            "salesman_code_field_name": "ti_Codigo_Vendedor_Balanceados__c"
        },
        "SANTA_REYES": {
            "client_id_field_name": "ti_Id_PV_ERP_Italhuevo__c",
            "email_field_name": "ti_correos_electronicos_Italhuevo__c",
            "salesman_field_name": "ti_Nombre_Vendedor_Italhuevo__c",
            "salesman_code_field_name": "ti_Codigo_Vendedor_Italhuevo__c"
        }
    }

    fields = COMPANY_FIELD_MAPS.get(selected_company)
    if not fields:
        raise ValueError(f"Unsupported company: {selected_company}")

    client_id_field_name = fields["client_id_field_name"]
    email_field_name = fields["email_field_name"]
    salesman_field_name = fields["salesman_field_name"]
    salesman_code_field_name = fields["salesman_code_field_name"]
    
    # List to be populated with mapped data from views
    mapped_list = []
    for idx, row in enumerate(db_rows):
        row_body = {
            "AccountId" : "",  # TODO retrieved from dynamo
            client_id_field_name : safe_get(row, "f201_id_sucursal", idx),
            "Name" : safe_get(row, "des_sucursal", idx),#Nombre punto de envio
            "ti_nombre_comercial__c" : safe_get(row, "f201_descripcion_sucursal", idx),
            #"ti_bodega__c" : safe_get(row, "f206_descripcion", idx), #Bodega
            #"Street" : safe_get(row, "f015_direccion1", idx),
            "City": safe_get(row, "f013_descripcion", idx),
            "State" : safe_get(row, "f012_descripcion", idx),
            "Country" : safe_get(row, "f011_descripcion", idx),
            #"PostalCode" : safe_get(row, "f015_cod_postal", idx),
            "OwnerId" : "005DZ00000AbiuB", #Vendedor
            "cu_Prioridad__c" : safe_get(row, "Prioridad", idx), #Prioridad CL-
            "cu_clasificacion_cliente__c" : safe_get(row, "Prioridad", idx), #Clasificacion CL-024
            "cu_tipologia_tienda__c" : safe_get(row, "Tipologia_tienda", idx), #Tipologia CL-022
            "cu_canal__c" : safe_get(row, "Subcategoria", idx), #Canal CL-002
            "cu_subcanal__c" : safe_get(row, "Canal", idx), #Subcanal CL-023
            "ti_lista_precio_italhuevo__c" : safe_get(row, "f201_id_lista_precio", idx),
            salesman_field_name : safe_get(row, "Nombre_vendedor", idx),
            salesman_code_field_name: safe_get(row, "id_vendedor", idx),
            #"ti_grupo_descuento__c" : safe_get(row, "f201_id_grupo_dscto", idx),
            "cu_plazo__c" : safe_get(row, "f201_cond_pago_desc", idx),
            "cu_cupo__c" : safe_get(row, "f201_cupo_credito", idx),
            "cu_descuentos__c" : safe_get(row, "f201_tasa_dscto_global_cap", idx),
            email_field_name : safe_get(row, "f015_email", idx), # Falta
            "id_sucursal": safe_get(row, "f201_id_sucursal", idx),
            "ti_Regional__c": safe_get(row, "Nombre_Regional", idx),
            "ti_Descripcion_Condicion_de_Pago__c": safe_get(row, "f201_cond_pago_desc", idx),
            "ti_Codigo_Condicion_de_Pago__c": safe_get(row, "f201_id_cond_pago", idx),
            "ti_Unidad_Negocio__c": f"{row["f201_id_un_movto_factura"]}-{row["f281_descripcion"]}",
            "ti_Centro_Costos__c": "",
            "ti_Tipo_Cliente__c": safe_get(row, "f201_id_tipo_cli", idx),
            "ti_portafolio__c": "", #Portafolio falta
        }
        mapped_list.append(row_body)    
    return mapped_list

def map_acuerdos_from_view(selected_company, db_rows):
    # List to be populated with mapped data from views
    mapped_list = []
    for row in db_rows:
        row_body = {
            #"AccountId" : "",  # TODO retrieved from dynamo
            # client_id_field_name : row["f200_nit"],
            "Id_AC_ERP_balanceados__c":f"{row["f200_nit"]}-{row["f201_id_sucursal"]}", #Concatenar
            "Name" : f"{row["f201_id_sucursal"]}-{row["f201_descripcion_sucursal"]}",
            "ac_cuenta__c": "", #ID_account_salesforce 
            "ac_vendedor__c": row["f200_razon_social"],#Nom_vendedor
            "ac_codigo_vendedor__c": row["f210_id"],#Codigo vendedor
            "ac_tipo_cliente__c": f"{row["f201_id_tipo_cli"]}-{row["f278_descripcion"]}",
            "ac_grupo_descuentos__c": f"{row["f201_id_grupo_dscto"]}-{row["f109_descripcion"]}",
            "ac_Lista_precio__c": f"{row["f201_id_lista_precio"]}-{row["f112_descripcion"]}",
            "ac_condiciones_pago__c":f"{row["f208_id"]}-{row["f208_descripcion"]}",
            "ac_plazo__c": row["f208_dias_vcto"],
            "ac_Unidades_de_negocio__c": f"{row["f201_id_un_movto_factura"]}-{row["f281_descripcion"]}",
            "ac_canal__c": row["v207_descripcion_C"],
            "ac_Correo_Electronico__c": row["FE_Email"],
            "OwnerId": "005DZ00000AbiuB",
            "ac_Linea_que_maneja__c": row["v207_descripcion_L"],
            "ac_estado__c": row["f201_ind_estado_activo"],
        }
        mapped_list.append(row_body)    
    return mapped_list


def safe_get(row, key, row_index):
    if key not in row or row[key] is None:
        raise KeyError(f"Missing required field '{key}' in row {row_index}")
    return row[key]