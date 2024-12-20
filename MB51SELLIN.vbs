
If Not IsObject(application) Then
   Set SapGuiAuto  = GetObject("SAPGUI")
   Set application = SapGuiAuto.GetScriptingEngine
End If
If Not IsObject(connection) Then
   Set connection = application.Children(0)
End If
If Not IsObject(session) Then
   Set session    = connection.Children(0)
End If
If IsObject(WScript) Then
   WScript.ConnectObject session,     "on"
   WScript.ConnectObject application, "on"
End If
session.findById("wnd[0]").maximize
session.findById("wnd[0]/tbar[0]/okcd").text = "mb51"
session.findById("wnd[0]").sendVKey 0
session.findById("wnd[0]/tbar[1]/btn[17]").press
session.findById("wnd[1]/usr/txtV-LOW").text = "vtasellin"
session.findById("wnd[1]/usr/txtENAME-LOW").text = "etorres"
session.findById("wnd[1]/usr/txtV-LOW").caretPosition = 9
session.findById("wnd[1]/tbar[0]/btn[8]").press
session.findById("wnd[0]/usr/ctxtBUDAT-LOW").text = "27052024"
session.findById("wnd[0]/usr/ctxtBUDAT-HIGH").text = "02062024"
session.findById("wnd[0]/usr/ctxtBUDAT-HIGH").setFocus
session.findById("wnd[0]/usr/ctxtBUDAT-HIGH").caretPosition = 8
session.findById("wnd[0]/tbar[1]/btn[8]").press
session.findById("wnd[0]/mbar/menu[0]/menu[1]/menu[2]").select
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select
session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").setFocus
session.findById("wnd[1]/tbar[0]/btn[0]").press
session.findById("wnd[1]/usr/ctxtDY_PATH").text = "C:\Users\Etorres\Desktop\"
session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "VENTASELLIN.CSV"
session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 15
session.findById("wnd[1]/tbar[0]/btn[11]").press
session.findById("wnd[0]").sendVKey 3
session.findById("wnd[0]").sendVKey 3
