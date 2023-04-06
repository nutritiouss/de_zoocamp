# Course Project - Netherland energy


## Grafana
Password to access grafana via web inerface is ```"admin:admin"```

- Go to grafana's datasoruces```http://<ip_your_vm_instance>:3000/datasources```
- Add you key to BigQuery datasource (google_credentials.json)
- Select processing location  “London(europe-west2)” (Should be consistent with Terraform varibles.tf )
- Push the button “Save and test”
- Navigate to Dashbords>Netherlands-energy 
(```<ip_your_vm_instance>:3000/d/d9e6FtY4z/netherlands-energy?orgId=1```)
- Enjoy!
