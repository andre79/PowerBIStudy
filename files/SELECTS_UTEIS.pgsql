SELECT 
    cpf,
    rg,
    nome_completo,

    case
        when estado_civil = 'S' then 'Solteiro(a)'
        when estado_civil = 'C' then 'Casado(a)'
        when estado_civil = 'D' then 'Divorciado(a)'
        when estado_civil = 'V' then 'Viúvo(a)'
        else 'Outro'
    end AS estado_civil_convertido,
    login,
    (DATE '1900-01-01' + (data_de_nascimento::integer - 2)) AS data_de_nascimento_convertida,
    (DATE '1900-01-01' + (data_de_contratacao::integer - 2)) AS data_de_contratacao_convertida,
    (DATE '1900-01-01' + (data_de_demissao::integer - 2)) AS data_de_demissao_convertida,
    dias_uteis_trabalhados_ano_orcamentario,
    salario_base,
    impostos
FROM 
    "TAB_FUNCIONÁRIOSANTIGOS";


SELECT 
    *
FROM 
    "TAB_FUNCIONÁRIOSANTIGOS"