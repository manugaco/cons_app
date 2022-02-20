select exists (
    select from information_schema.tables
    where table_schema = 'smi_schema'
    and table_name = 'smi_corpus'
)