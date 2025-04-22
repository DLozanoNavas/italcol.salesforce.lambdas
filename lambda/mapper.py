def map_from_view(selected_company, db_rows):
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
            client_id_field_name: safe_get(row, "F200_nit", idx),
            "AccountNumber": safe_get(row, "F200_nit", idx),
            "Type": "Customer",
            "Industry": "Retail",
            "ShippingCity": safe_get(row, "f011_descripcion", idx),
            "ShippingCountry": safe_get(row, "f013_descripcion", idx),
            "ShippingStreet": safe_get(row, "f015_direccion1", idx),
            "cu_CIIU__c": safe_get(row, "f200_id_ciiu", idx),
        }

        tipo_ident = safe_get(row, "f200_id_tipo_ident", idx)

        if tipo_ident == "N":
            row_body["Name"] = safe_get(row, "f200_razon_social", idx)
            row_body["cu_colaborador_grupo_italcol__c"] = False
        elif tipo_ident in ("C", "E"):
            row_body["FirstName"] = safe_get(row, "f200_nombres", idx)
            row_body["LastName"] = f"{safe_get(row, 'f200_apellido1', idx)} {safe_get(row, 'f200_apellido2', idx)}"
            row_body["Salutation"] = "Sr."
            row_body["cu_colaborador_grupo_italcol__c"] = True

            if safe_get(row, "f200_ind_empleado", idx) == 1:
                row_body["co_compania__pc"] = selected_company

            mapped_list.append(row_body)
    return mapped_list

def safe_get(row, key, row_index):
    if key not in row or row[key] is None:
        raise KeyError(f"Missing required field '{key}' in row {row_index}")
    return row[key]