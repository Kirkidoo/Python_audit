import streamlit as st
import pandas as pd
from ftp_service import list_csv_files, get_csv_as_dataframe
from shopify_service import get_shopify_data_for_skus, get_shopify_data_bulk, get_shopify_locations, update_product_tags, update_product_template_suffix, update_variant_price, create_product, remove_product_tag, batch_process_mismatches
from audit_engine import check_mismatches, check_stale_clearance

st.set_page_config(page_title="SyncShop Audit", page_icon="📝", layout="wide")

st.title("SyncShop Local Audit Dashboard")

@st.cache_data(ttl=300)
def fetch_ftp_files():
    return list_csv_files()

@st.cache_data(ttl=600)
def fetch_locations():
    try:
        return get_shopify_locations()
    except Exception:
        return []

# Sidebar for file selection
st.sidebar.header("Data Source")
try:
    files = fetch_ftp_files()
except Exception as e:
    st.sidebar.error("Could not connect to FTP.")
    files = []

if not files:
    st.sidebar.warning("No CSV files found in FTP.")
else:
    selected_file = st.sidebar.selectbox("Select CSV to Audit", files)
    
    default_index = 1 if "ShopifyProductImport.csv" in selected_file else 0
    fetch_method = st.sidebar.radio("Data Fetch Method", 
                                    ["Standard (Fast)", "Bulk Operation (For Large Files)"],
                                    index=default_index,
                                    help="Bulk Operation handles large FTP files (like 50,000+ SKUs) efficiently.")
    
    # --- Location Controls ---
    all_locations = fetch_locations()
    location_names = [loc['name'] for loc in all_locations]
    location_name_to_id = {loc['name']: loc['id'] for loc in all_locations}
    
    if all_locations:
        selected_location_names = st.sidebar.multiselect(
            "Inventory Locations",
            options=location_names,
            default=location_names,
            help="Select which locations to show per-location inventory for. Leave all selected to see all."
        )
        selected_locations = [
            {"id": location_name_to_id[n], "name": n}
            for n in selected_location_names
        ]
    else:
        selected_locations = []
        st.sidebar.caption("⚠️ Could not load locations from Shopify.")
    
    # Bulk-only: opt-in to per-location inventory
    include_locations_bulk = False
    if "Bulk" in fetch_method and all_locations:
        include_locations_bulk = st.sidebar.checkbox(
            "Include per-location inventory (slower bulk query)",
            value=False,
            help="Extends the bulk export to include inventory quantities per location. Increases time and file size."
        )
    
    if st.sidebar.button("Run Audit", type="primary"):
        st.session_state['run_audit'] = True
        # Clear previous data when running a new audit
        st.session_state.pop('mismatch_df', None)
        st.session_state.pop('missing_df', None)
        st.session_state.pop('matched_count', None)
        st.session_state.pop('missing_count', None)
        
    if st.session_state.get('run_audit', False):
        if 'mismatch_df' not in st.session_state:
            with st.spinner(f"Downloading {selected_file} from FTP..."):
                try:
                    csv_df = get_csv_as_dataframe(selected_file)
                except Exception as e:
                    st.error(f"Failed to download or parse CSV: {e}")
                    st.stop()
                    
            if csv_df.empty:
                st.warning("The selected CSV file is empty.")
                st.stop()
                
            skus = csv_df['sku'].dropna().unique().tolist()
            
            with st.spinner(f"Fetching {len(skus)} SKUs from Shopify..."):
                try:
                    if "Bulk" in fetch_method:
                        shopify_df, excessive_media_df = get_shopify_data_bulk(skus, include_locations=include_locations_bulk)
                    else:
                        shopify_df = get_shopify_data_for_skus(skus, locations=selected_locations if selected_locations else None)
                        excessive_media_df = pd.DataFrame()
                    st.session_state['is_bulk_mode'] = "Bulk" in fetch_method
                except Exception as e:
                    st.error(f"Failed to fetch Shopify data: {e}")
                    st.stop()
                    
            with st.spinner("Running discrepancy engine..."):
                mismatch_df, missing_df, matched_count = check_mismatches(csv_df, shopify_df, selected_file)
                
                if 'clearance' in selected_file.lower():
                    stale_df = check_stale_clearance(shopify_df, csv_df)
                    if not stale_df.empty:
                         mismatch_df = pd.concat([mismatch_df, stale_df], ignore_index=True)
                         
            mismatch_df['Error_Log'] = ""             
            st.session_state['mismatch_df'] = mismatch_df
            st.session_state['missing_df'] = missing_df
            st.session_state['excessive_media_df'] = excessive_media_df
            st.session_state['matched_count'] = matched_count
            st.session_state['missing_count'] = len(missing_df)
            st.success("Audit complete!")
            
        mismatch_df = st.session_state['mismatch_df']
        missing_df = st.session_state.get('missing_df', pd.DataFrame())
        excessive_media_df = st.session_state.get('excessive_media_df', pd.DataFrame())
        is_bulk = st.session_state.get('is_bulk_mode', False)
        matched_count = st.session_state['matched_count']
        missing_count = st.session_state['missing_count']
        
        # Dashboard Metrics
        total_mismatches = len(mismatch_df)
        col1, col2, col3 = st.columns(3)
        col1.metric("Matched SKUs", matched_count)
        col2.metric("Missing in Shopify", missing_count)
        col3.metric("Total Mismatches", total_mismatches, delta_color="inverse")
        
        st.divider()
        
        if 'last_action_success' in st.session_state:
            st.success(st.session_state.pop('last_action_success'))
        if 'last_action_errors' in st.session_state:
            error_logs = st.session_state.pop('last_action_errors')
            with st.expander(f"View {len(error_logs)} Errors", expanded=True):
                for log in error_logs:
                    st.error(log)

        def process_fixes(df_to_fix):
            if df_to_fix.empty:
                st.info("No items selected to fix.")
                return
            
            with st.spinner(f"Batch processing {len(df_to_fix)} fixes..."):
                errors_dict = batch_process_mismatches(df_to_fix)
            
            success_count = len(df_to_fix) - len(errors_dict)
            error_count = len(errors_dict)
            
            # Identify which rows succeeded
            successful_indices = [idx for idx in df_to_fix.index if idx not in errors_dict]
            
            if success_count > 0:
                st.session_state['mismatch_df'] = st.session_state['mismatch_df'].drop(index=successful_indices)
                st.session_state['last_action_success'] = f"Successfully processed {success_count} items."
            
            if error_count > 0:
                # Update the Error_Log column for failed items
                for idx, err_msg in errors_dict.items():
                    if idx in st.session_state['mismatch_df'].index:
                         st.session_state['mismatch_df'].at[idx, 'Error_Log'] = err_msg
                         
                st.session_state['last_action_errors'] = [f"{error_count} items failed. Check the 'Error_Log' column for details."]
                
            if success_count > 0 or error_count > 0:
                st.rerun()
                
        def process_creations(df_to_create):
            if df_to_create.empty:
                st.info("No items selected to create.")
                return
            
            # Ensure every row has a handle. If missing, use SKU as a fallback handle
            # to prevent grouping unrelated blank-handle products together.
            df_to_create['handle'] = df_to_create.apply(
                lambda row: row['sku'] if pd.isna(row.get('handle')) or not str(row.get('handle')).strip() else row['handle'],
                axis=1
            )
            
            # Group by handle to create variants under the same product
            grouped = df_to_create.groupby('handle')
            total_products = len(grouped)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            success_count = 0
            error_count = 0
            error_logs = []
            successful_indices = []
            
            for i, (handle, group) in enumerate(grouped):
                status_text.text(f"Creating {i+1} of {total_products}: Product {handle} ({len(group)} variants)")
                
                try:
                    # Parent product info comes from the first row in the group
                    first_row = group.iloc[0]
                    title = first_row.get('title', handle) if pd.notna(first_row.get('title')) and first_row.get('title') else handle
                    tags = first_row.get('tags')
                    template_suffix = first_row.get('templateSuffix')
                    product_type = first_row.get('type')
                    vendor = first_row.get('vendor')
                    body_html = first_row.get('body_html')
                    seo_title = first_row.get('seo_title')
                    seo_description = first_row.get('seo_description')
                    
                    # Build variants list for this product
                    variants = []
                    for _, row in group.iterrows():
                        variants.append({
                            'sku': row['sku'], 
                            'price': row['price'], 
                            'compareAtPrice': row['compareAtPrice'], 
                            'weight': row.get('grams') if pd.notna(row.get('grams')) and row.get('grams') != '' else row.get('weight', ''),
                            'weightUnit': row.get('weightUnit', ''),
                            'barcode': row.get('barcode', ''),
                            'option1_name': row.get('option1_name', ''),
                            'option1_value': row.get('option1_value', ''),
                            'option2_name': row.get('option2_name', ''),
                            'option2_value': row.get('option2_value', ''),
                            'option3_name': row.get('option3_name', ''),
                            'option3_value': row.get('option3_value', '')
                        })
                        
                    success, error_msg = create_product(
                        title=title, 
                        variants=variants, 
                        tags=tags, 
                        template_suffix=template_suffix,
                        product_type=product_type,
                        vendor=vendor,
                        body_html=body_html,
                        seo_title=seo_title,
                        seo_description=seo_description
                    )
                except Exception as e:
                    success, error_msg = False, str(e)
                    
                if success:
                    success_count += len(group)
                    successful_indices.extend(group.index.tolist())
                    st.session_state['missing_count'] = max(0, st.session_state['missing_count'] - len(group))
                else:
                    error_count += len(group)
                    error_logs.append(f"**Handle {handle}**: {error_msg}")
                    
                progress_bar.progress((i + 1) / total_products)
                
            status_text.text(f"Finished! Success: {success_count} variants, Errors: {error_count} variants")
            
            if success_count > 0:
                st.session_state['missing_df'] = st.session_state['missing_df'].drop(index=successful_indices)
                st.session_state['last_action_success'] = f"Successfully created {success_count} variants across {total_products} new products."
            
            if error_count > 0:
                st.session_state['last_action_errors'] = error_logs
                
            if success_count > 0 or error_count > 0:
                st.rerun()
        
        if is_bulk:
            tabs = st.tabs(["Mismatch Report", "Missing Products Report", "Excessive Media"])
        else:
            tabs = st.tabs(["Mismatch Report", "Missing Products Report"])
        
        with tabs[0]:
            if total_mismatches > 0:
                st.subheader("Mismatch Report")
                
                # Filtering
                fields = ["All"] + list(mismatch_df['field'].unique())
                
                selected_field = st.radio("Filter by Issue Type", fields, horizontal=True, key="mismatch_filter")
                
                if selected_field != "All":
                     display_df = mismatch_df[mismatch_df['field'] == selected_field].copy()
                else:
                     display_df = mismatch_df.copy()
                     
                # "Select All" checkbox placed in a narrow column to visually align with the Select column header
                _sel_col, _ = st.columns([1, 14])
                with _sel_col:
                    select_all_mismatch = st.checkbox("All", key="select_all_mismatch", help="Select / deselect all rows")
                
                # Add Select column for data editor
                display_df.insert(0, 'Select', select_all_mismatch)
                     
                # Determine which location qty columns exist in the data
                loc_qty_cols = [c for c in display_df.columns if c.endswith(' Qty')]
                
                # Hidden system columns (never shown to user)
                hidden_cols = {
                    "variant_id": None,
                    "product_id": None,
                    "inventory_item_id": None,
                    "shopify_price": None,
                    "shopify_compare_at_price": None,
                    "is_clearance_file": None
                }
                
                # Location qty columns: visible but read-only
                loc_col_config = {
                    col: st.column_config.NumberColumn(col, help=f"On-hand inventory at {col.replace(' Qty', '')}")
                    for col in loc_qty_cols
                }
                
                column_config = {
                    "Select": st.column_config.CheckboxColumn('Select', default=False),
                    **hidden_cols,
                    **loc_col_config
                }
                
                edited_df = st.data_editor(
                    display_df,
                    hide_index=True,
                    column_config=column_config,
                    disabled=[col for col in display_df.columns if col != 'Select'],
                    use_container_width=True,
                    height=600,
                    key="mismatch_editor"
                )
                
                # Sub-actions
                st.markdown("### Bulk Actions")
                col_a, col_b = st.columns(2)
                
                selected_rows = edited_df[edited_df['Select']]
                
                # If "Select All" is checked, Fix Selected acts like Fix All
                rows_to_fix = edited_df if select_all_mismatch else selected_rows
                fix_selected_disabled = rows_to_fix.empty
                
                if col_a.button("⚙️ Fix Selected", type="primary", disabled=fix_selected_disabled, key="fix_btn"):
                    process_fixes(rows_to_fix)
                    
                if col_b.button("🚨 Fix All", type="secondary", key="fix_all_btn"):
                    process_fixes(edited_df)
                    
                st.divider()
                
                # Download Button — strip location qty cols from download too for cleanliness (user can opt out)
                download_df = mismatch_df.drop(columns=['variant_id', 'product_id', 'inventory_item_id', 'shopify_price', 'shopify_compare_at_price', 'is_clearance_file'], errors='ignore')
                csv_data = download_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Mismatch Report as CSV",
                    data=csv_data,
                    file_name=f"audit_report_{selected_file}",
                    mime="text/csv",
                    key="dl_mismatch"
                )
            else:
                st.info("🎉 All products match perfectly! No mismatches to fix.")
                
        with tabs[1]:
            if not missing_df.empty:
                 st.subheader("Missing Products - Ready to Create")
                 st.markdown("Review and edit product details before creation. Changes here map directly to Shopify fields.")
                 
                 display_missing_df = missing_df.copy()
                 
                 select_all_missing = st.checkbox("Select All", key="select_all_missing")
                 display_missing_df.insert(0, 'Select', select_all_missing)
                 
                 edited_missing_df = st.data_editor(
                     display_missing_df,
                     hide_index=True,
                     column_config={"Select": st.column_config.CheckboxColumn('Select', default=False)},
                     use_container_width=True,
                     height=600,
                     key="missing_editor"
                 )
                 
                 st.markdown("### Bulk Actions")
                 col_c, col_d = st.columns(2)
                 
                 selected_missing_rows = edited_missing_df[edited_missing_df['Select']]
                 
                 if col_c.button("✨ Create Selected", type="primary", disabled=selected_missing_rows.empty, key="create_btn"):
                      process_creations(selected_missing_rows)
                      
                 if col_d.button("🚨 Create All", type="secondary", key="create_all_btn"):
                      process_creations(edited_missing_df)
                      
                 st.divider()
                 csv_missing_data = missing_df.to_csv(index=False).encode('utf-8')
                 st.download_button(
                     label="Download Missing Products as CSV",
                     data=csv_missing_data,
                     file_name=f"missing_products_{selected_file}",
                     mime="text/csv",
                     key="dl_missing"
                 )
            else:
                 st.info("No missing products found. All SKUs in CSV exist in Shopify.")

        if is_bulk:
            with tabs[2]:
                st.subheader("Excessive Media Analyzer")
                st.markdown("Products where the number of media items exceeds the number of variants. Evaluated across the **entire Shopify catalog**.")
                if not excessive_media_df.empty:
                    st.dataframe(excessive_media_df, use_container_width=True, hide_index=True)
                    st.divider()
                    csv_media = excessive_media_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Excessive Media Report as CSV",
                        data=csv_media,
                        file_name=f"excessive_media_{selected_file.replace('.csv', '')}.csv" if selected_file else "excessive_media.csv",
                        mime="text/csv",
                        key="dl_excessive_media"
                    )
                else:
                    st.info("🎉 No products found with excessive media!")
