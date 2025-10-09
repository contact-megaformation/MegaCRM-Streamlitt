File "/mount/src/megacrm-streamlitt/MegaCRM_Streamlit.py", line 235, in <module>
    st.dataframe(df_view[cols_show] if not df_view.empty else pd.DataFrame(columns=cols_show), use_container_width=True)
    ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/home/adminuser/venv/lib/python3.13/site-packages/streamlit/runtime/metrics_util.py", line 443, in wrapped_func
    result = non_optional_func(*args, **kwargs)
File "/home/adminuser/venv/lib/python3.13/site-packages/streamlit/elements/arrow.py", line 706, in dataframe
    proto.data = dataframe_util.convert_pandas_df_to_arrow_bytes(data_df)
                 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^
File "/home/adminuser/venv/lib/python3.13/site-packages/streamlit/dataframe_util.py", line 822, in convert_pandas_df_to_arrow_bytes
    table = pa.Table.from_pandas(df)
File "pyarrow/table.pxi", line 4795, in pyarrow.lib.Table.from_pandas
File "/home/adminuser/venv/lib/python3.13/site-packages/pyarrow/pandas_compat.py", line 594, in dataframe_to_arrays
    convert_fields) = _get_columns_to_convert(df, schema, preserve_index,
                      ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                              columns)
                                              ^^^^^^^^
File "/home/adminuser/venv/lib/python3.13/site-packages/pyarrow/pandas_compat.py", line 374, in _get_columns_to_convert
    raise ValueError(
        f'Duplicate column names found: {list(df.columns)}'
    )
