# 📊 Pipeline PJe — Extração, Higienização e Classificação

Este projeto implementa uma pipeline completa para coleta, tratamento e análise de comunicações judiciais a partir da API pública do Comunica PJe.

A solução é dividida em **3 fases principais**:

* **Fase 1 — Extração** → Consome a API e salva dados brutos (JSON)
* **Fase 2 — Higienização** → Limpa e padroniza os dados (CSV)
* **Fase 3 — Consolidação** → Classifica e gera a planilha final (Excel)

---

## 📁 Estrutura do Projeto

```
.
├── fase1_extracao.py
├── fase2_higienizacao.py
├── fase3_consolidacao.py
├── processos.txt
├── dados_brutos/
│   ├── comunicacoes_raw.json
│   ├── comunicacoes_higienizadas.csv
│   └── alertas_extracao.json
├── pipeline.log
└── pipeline_pje_output.xlsx
```

---

## ⚙️ Pré-requisitos

* Python 3.10+
* Instalar dependências:

```bash
pip install requests pandas openpyxl
```

---

## ▶️ Como Executar (Passo a Passo)

### 1. Criar arquivo de entrada

Crie um arquivo chamado `processos.txt` com um número de processo por linha:

```
0000000-00.0000.0.00.0000
1111111-11.1111.1.11.1111
```

---

### 2. Executar Fase 1 — Extração

```bash
python fase1_extracao.py
```

**Saída:**

* `dados_brutos/comunicacoes_raw.json`
* `dados_brutos/alertas_extracao.json`

---

### 3. Executar Fase 2 — Higienização

```bash
python fase2_higienizacao.py
```

**Saída:**

* `dados_brutos/comunicacoes_higienizadas.csv`

---

### 4. Executar Fase 3 — Consolidação

```bash
python fase3_consolidacao.py
```

**Saída final:**

* `pipeline_pje_output.xlsx`

---

## 🚀 Como Rodar a Pipeline Completa (Automatizado)

Se quiser rodar tudo de uma vez, você pode criar um arquivo `main.py`:

```python
from fase1_extracao import executar as fase1
from fase2_higienizacao import executar as fase2
from fase3_consolidacao import executar as fase3

# Fase 1
res_f1 = fase1()

# Fase 2
df, alertas_f2 = fase2(res_f1["comunicacoes"])

# Junta alertas
alertas_total = res_f1["alertas"] + alertas_f2

# Fase 3
arquivo_final = fase3(df, alertas_total)

print("Pipeline concluída:", arquivo_final)
```

Executar:

```bash
python main.py
```

---

## 📊 Estrutura da Planilha Final

O arquivo `pipeline_pje_output.xlsx` contém:

### Aba 1 — Comunicações Classificadas

* Texto higienizado
* Temas identificados
* Score
* Evidências

### Aba 2 — Resumo por Processo

* Score total
* Score máximo
* Principais temas
* Última movimentação
* Observação automática

### Aba 3 — Inconsistências e Alertas

* Problemas encontrados durante o processamento

---

## 🧠 Lógica de Classificação

A classificação é baseada em:

* **Palavras-chave (regex)**
* **Pesos por tema**
* **Bônus por combinação de temas**

Exemplo:

* "Homologação + Pagamento" → score mais alto

---

## ⚠️ Observações

* O sistema trata erros de API (timeout, rate limit, etc.)
* Remove duplicatas automaticamente
* Padroniza campos inconsistentes da API
* Gera logs em `pipeline.log`

---

## 📌 Resultado Esperado

Ao final da execução, você terá:

✔ Dados estruturados
✔ Classificação automática
✔ Planilha pronta para análise

---

## 👨‍💻 Autor

Projeto desenvolvido como teste técnico para avaliação de habilidades em:

* Python
* Engenharia de dados
* Tratamento de dados
* Estruturação de pipelines
