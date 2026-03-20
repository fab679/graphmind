import { Search, Info } from "lucide-react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { PropertyInspector } from "@/components/inspector/PropertyInspector";
import { SchemaBrowser } from "@/components/inspector/SchemaBrowser";

export function RightPanel() {
  return (
    <div className="flex h-full flex-col bg-background">
      <Tabs defaultValue="inspector" className="flex h-full flex-col">
        <TabsList>
          <TabsTrigger value="inspector">
            <span className="flex items-center gap-1">
              <Info className="h-3 w-3" />
              Inspector
            </span>
          </TabsTrigger>
          <TabsTrigger value="schema">
            <span className="flex items-center gap-1">
              <Search className="h-3 w-3" />
              Schema
            </span>
          </TabsTrigger>
        </TabsList>

        <TabsContent value="inspector" className="flex-1 min-h-0">
          <PropertyInspector />
        </TabsContent>

        <TabsContent value="schema" className="flex-1 min-h-0">
          <SchemaBrowser />
        </TabsContent>
      </Tabs>
    </div>
  );
}
