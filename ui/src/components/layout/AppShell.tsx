import { ActivityBar } from "@/components/layout/ActivityBar";
import { Navbar } from "@/components/layout/Navbar";
import { QueryTab } from "@/components/tabs/QueryTab";
import { ExploreTab } from "@/components/tabs/ExploreTab";
import { NlqTab } from "@/components/tabs/NlqTab";
import { SchemaTab } from "@/components/tabs/SchemaTab";
import { AdminTab } from "@/components/tabs/AdminTab";
import { SettingsTab } from "@/components/tabs/SettingsTab";
import { useUiStore } from "@/stores/uiStore";

export function AppShell() {
  const activeTab = useUiStore((s) => s.activeTab);

  return (
    <div className="flex h-screen bg-background text-foreground">
      <ActivityBar />
      <div className="flex flex-1 flex-col min-w-0">
        <Navbar />
        <main className="flex-1 min-h-0 overflow-hidden">
          {activeTab === "query" && <QueryTab />}
          {activeTab === "explore" && <ExploreTab />}
          {activeTab === "nlq" && <NlqTab />}
          {activeTab === "schema" && <SchemaTab />}
          {activeTab === "admin" && <AdminTab />}
          {activeTab === "settings" && <SettingsTab />}
        </main>
      </div>
    </div>
  );
}
