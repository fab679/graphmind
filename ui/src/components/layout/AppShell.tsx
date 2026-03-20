import { useState } from "react";
import {
  ResizablePanelGroup,
  ResizablePanel,
  ResizableHandle,
} from "@/components/ui/resizable";
import { Navbar } from "@/components/layout/Navbar";
import { LeftPanel } from "@/components/layout/LeftPanel";
import { BottomPanel } from "@/components/results/BottomPanel";
import { RightPanel } from "@/components/inspector/RightPanel";
import { ForceGraph } from "@/components/graph/ForceGraph";
import { FullscreenExplorer } from "@/components/graph/FullscreenExplorer";
import { useUiStore } from "@/stores/uiStore";

export function AppShell() {
  const rightPanelOpen = useUiStore((s) => s.rightPanelOpen);
  const bottomPanelOpen = useUiStore((s) => s.bottomPanelOpen);
  const [fullscreen, setFullscreen] = useState(false);

  return (
    <div className="flex h-screen flex-col bg-background text-foreground">
      <Navbar />

      <ResizablePanelGroup orientation="horizontal" className="flex-1">
        <ResizablePanel defaultSize="20%" minSize="180px">
          <LeftPanel />
        </ResizablePanel>

        <ResizableHandle withHandle />

        <ResizablePanel minSize="300px">
          <div className="relative flex flex-col h-full min-h-0">
            <div className="flex-1 min-h-0">
              <ForceGraph onFullscreen={() => setFullscreen(true)} />
            </div>
            {bottomPanelOpen && (
              <div className="absolute bottom-0 left-0 right-0 z-10 max-h-[50%] border-t bg-background overflow-auto">
                <BottomPanel />
              </div>
            )}
          </div>
        </ResizablePanel>

        {rightPanelOpen && (
          <>
            <ResizableHandle withHandle />

            <ResizablePanel defaultSize="20%" minSize="200px">
              <RightPanel />
            </ResizablePanel>
          </>
        )}
      </ResizablePanelGroup>

      {/* Fullscreen exploration overlay */}
      <FullscreenExplorer open={fullscreen} onClose={() => setFullscreen(false)} />
    </div>
  );
}
